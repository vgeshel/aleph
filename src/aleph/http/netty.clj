;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns aleph.http.netty
  (:use
    [aleph.http.core]
    [aleph netty formats]
    [aleph.netty.core :only (local-options)]
    [lamina core api connections trace executor])
  (:require
    [aleph.http.websocket :as ws]
    [aleph.http.client-middleware :as middleware]
    [aleph.netty.client :as client]
    [aleph.netty :as netty]
    [aleph.formats :as formats]
    [aleph.http.core :as http]
    [aleph.http.options :as options]
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     TimeoutException TimeUnit]
    [org.jboss.netty.handler.codec.http
     HttpHeaders
     DefaultHttpChunk
     HttpChunk
     HttpMessage
     HttpRequestDecoder
     HttpResponseEncoder
     HttpContentCompressor
     HttpContentDecompressor
     HttpClientCodec]
    [org.jboss.netty.handler.timeout
     ReadTimeoutHandler
     WriteTimeoutHandler]
    [java.nio.channels
     ClosedChannelException]))

;;;

(defn wrap-http-server-channel [options ch]
  (let [netty-channel (netty/network-channel->netty-channel ch)
        responses (channel)
        auto-decode? (options/auto-decode?)]

    ;; transform responses
    (join
      (http/expand-writes http/ring-map->netty-response true responses)
      ch)

    ;; transform requests
    (let [requests (->> ch
                     (http/collapse-reads netty-channel)
                     (map* http/netty-request->ring-map))
          requests (if auto-decode?
                     (map* http/decode-message requests)
                     requests)]
          (splice requests responses))))

(defn start-http-server [handler options]
  (let [server-name (or
                      (:name options)
                      (-> options :server :name)
                      "http-server")
        netty-options (-> options :netty)
        error-probe (error-probe-channel [server-name :error])
        channel-handler (server-generator
                          (fn [ch req]

                            ;; set local options
                            (.set local-options options)

                            (let [ch* (result-channel)]

                              ;; run the handler
                              (run-pipeline req
                                {:error-handler #(error ch* %)}
                                #(handler ch* %))

                              ;; handle the response
                              (run-pipeline ch*
                                {:error-handler (fn [_])}
                                #(enqueue ch (assoc % :keep-alive? (:keep-alive? req))))))
                          
                          (merge
                            {:error-response (fn [ex]
                                               (enqueue error-probe ex)
                                               (if (or
                                                     (= ex :lamina/timeout!)
                                                     (instance? TimeoutException ex))
                                                 {:status 408}
                                                 {:status 500}))}
                            (:server options)
                            {:name server-name}))]
    (netty/start-server
      server-name
      (fn [channel-group]
        (let [pipeline (netty/create-netty-pipeline server-name true channel-group
                         :decoder (HttpRequestDecoder.
                                    (get netty-options "http.maxInitialLineLength" 16384)
                                    (get netty-options "http.maxHeaderSize" 16384)
                                    (get netty-options "http.maxChunkSize" 16384))
                         :encoder (HttpResponseEncoder.)
                         :deflater (HttpContentCompressor.)
                         :handler (server-message-handler
                                    (fn [ch _]
                                      (->> ch
                                        (wrap-http-server-channel options)
                                        channel-handler))))]
          (when (options/websocket? options)
            (.addBefore pipeline "handler" "websocket"
              (ws/server-handshake-stage handler)))
          pipeline))
      options)))

;;;

(defn- log-drained [req-info label ch]
  (on-drained ch (fn []
                   (try
                     (log/warnf "drained: %s/%s" (:id @req-info) label)
                     (throw (RuntimeException.))
                     (catch Exception e
                       (swap! req-info
                              assoc-in
                              [:drains label]
                              {:ts (System/currentTimeMillis)
                               :stack e})))))
  ch)

(defn wrap-http-client-channel [options ch]
  (let [netty-channel (netty/network-channel->netty-channel ch)
        requests (channel)
        options (client/expand-client-options options)
        auto-decode? (options/auto-decode? options)
        req-info (::info options)
        log-drained (partial log-drained req-info)]

    ;; transform requests
    (join
      (->> requests
        (map*
          (fn [req]
            (let [req (client/expand-client-options req)
                  req (merge options req)
                  req (middleware/transform-request req)]
              (update-in req [:keep-alive?] #(if (nil? %) true %)))))
        (http/expand-writes http/ring-map->netty-request false))
      ch)

    ;; transform responses
    (let [responses (->> ch
                         (log-drained 1)
                         (http/collapse-reads netty-channel)
                         (log-drained 2)
                         (map* http/netty-response->ring-map)
                         (log-drained 3)
                         (map* #(dissoc % :keep-alive?))
                         (log-drained 4))
          responses (if auto-decode?
                      (log-drained 5 (map* http/decode-message responses))
                      responses)
          responses (if-let [frame (formats/options->decoder options)]
                      (log-drained
                       6
                       (map*
                        (fn [rsp]
                          (update-in rsp [:body]
                                     #(let [body (if (channel? %)
                                                   %
                                                   (closed-channel %))]
                                        (formats/decode-channel frame body))))
                        responses))
                      responses)
          ret (splice responses requests)]
      (on-closed ret (fn [] (.close netty-channel)))
      (log-drained 7 ret)
      ret)))

(def ^{:private true} timer
  (org.jboss.netty.util.HashedWheelTimer. 10 TimeUnit/MILLISECONDS))

(defn-instrumented http-connection-
  {:name "aleph:http-connection"}
  [options timeout]
  (let [client-name (or
                      (:name options)
                      (-> options :client :name)
                      "http-client")
        options (assoc options
                  :timeout timeout)]
    (run-pipeline nil
      {:error-handler (fn [_])
       :timeout timeout}
      (fn [_]
        (create-client
          client-name
          (fn [channel-group]
            (create-netty-pipeline client-name false channel-group
                                   :read-timeout (ReadTimeoutHandler. timer
                                                                      (long (or (:read-timeout options)
                                                                                timeout
                                                                                0))
                                                                      TimeUnit/MILLISECONDS)
                                   :write-timeout (WriteTimeoutHandler. timer
                                                                        (long (or (:write-timeout options)
                                                                                  timeout
                                                                                  0))
                                                                        TimeUnit/MILLISECONDS)
                                   :codec (HttpClientCodec.)
                                   :inflater (HttpContentDecompressor.)))
          options))
      (fn [connection]
        (let [ch (wrap-http-client-channel options connection)]
          ch)))))

(defn http-connection
  "Returns a channel representing an HTTP connection."
  ([options]
     (http-connection- options nil))
  ([options timeout]
     (http-connection- options timeout)))

(defn http-client [options]
  (client #(http-connection options)))

(defn pipelined-http-client [options]
  (pipelined-client #(http-connection options)))

(defonce counter (atom 0))

(defn-instrumented http-request-
  {:name "aleph:http-request"}
  [request timeout]
  (let [request (assoc request :keep-alive? false)
        start (System/currentTimeMillis)
        duration (fn [] (- (System/currentTimeMillis) start))
        elapsed (atom nil)
        sent (atom nil)
        received (atom nil)
        receive-timeout (atom nil)
        ch-atom (atom nil)
        req-info (atom {:id (swap! counter inc)})
        request (assoc request ::info req-info)]
    (log/infof "starting %s" @req-info)
    (run-pipeline request
      {:error-handler (fn [ex]
                        (when-let [ch @ch-atom]
                          (close ch))
                        (when-let [drains (:drains @req-info)]
                          (let [drains (sort-by (comp :ts second) drains)
                                [label {:keys [stack ts]}] (last drains)]
                            (log/errorf stack "failed request %s: drain %s" (:id @req-info "no-id") (or label "no-label"))))
                        (when (= :lamina/timeout! ex)
                          (if-let [elapsed @elapsed]
                            (complete (TimeoutException. (format "HTTP request %s timed out after %d ms, took %d ms to connect, req sent after %s, resp received after %s, timeout was %s"
                                                                 (:id @req-info)
                                                                 (duration)
                                                                 elapsed
                                                                 @sent
                                                                 @received
                                                                 @receive-timeout)))
                            (complete (TimeoutException. (format "HTTP request %s timed out trying to establish connection" (:id @req-info)))))))}
      http-connection
      (fn [ch]
        (reset! elapsed (duration))
        (when timeout
          (reset! receive-timeout (- timeout @elapsed)))
        (reset! ch-atom ch)
        (run-pipeline ch
          {:timeout @receive-timeout
           :error-handler (fn [ex]
                            ;; TODO: remove
                            (log/errorf "error consuming HTTP response %s, after %d ms: %s"
                                        (:id @req-info)
                                        (duration)
                                        ex)
                            (close ch))}
          (fn [ch]
            (enqueue ch request)
            (reset! sent (duration))
            (on-drained ch (fn []
                             (when-not @received
                               (log/warnf "drained before response is read. req-id %s after %d"
                                          (:id @req-info)
                                          (duration)))))
            (read-channel ch))
          (fn [rsp]
            (reset! received (duration))
            (if (channel? (:body rsp))
              (on-closed (:body rsp) #(close ch))
              (close ch))
            rsp))))))

(defn http-request
  "Takes an HTTP request, formatted per the Ring spec, and returns an async-result
   representing the server's response."
  ([request]
     (http-request- request nil))
  ([request timeout]
     (http-request- request timeout)))
