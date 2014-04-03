(ns task02.network
  (:use [task02 helpers query])
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.net Socket ServerSocket InetAddress InetSocketAddress SocketException]))

(def inactive-timeout 20000)

;; Объявить переменную для синхронизации между потоками. Воспользуйтесь promise
(def ^{:private true :dynamic true} *should-be-finished* nil)

(def server-socket (atom nil))

(def open-sockets (agent {}))

(defn closeconn-message
  [open-connections socket-id message]
  (when-let [{:keys [socket]} (get open-connections socket-id)]
    (try
      (binding [*out* (io/writer (.getOutputStream socket))]
        (println message))
      (.close socket)
      (catch Throwable t
        (log-t "" t)))
    (dissoc open-connections socket-id)))

(defn- shutdown-server
  []
  (try
    (.close @server-socket)
    (catch Throwable t
      (log-t t)))
  (reset! server-socket nil)
  (deliver *should-be-finished* true)
  (doseq [[socket-id _] @open-sockets]
    (send open-sockets closeconn-message socket-id "Server shutdown.")))

;; Hint: *in*, *out*, io/writer, io/reader, socket.getOutputStream(), socket.getInputStream(), socket.close(), binding
;;       deliver, prn
(defn handle-request [^Socket sock sock-id]
  (binding [*in* (io/reader (.getInputStream sock))
            *out* (io/writer (.getOutputStream sock))]
    (println "Educational SQL like Server. Welcome!")
    (try
      (doseq [s (line-seq *in*)
              :let [sl (str/lower-case s)]]
        (send open-sockets assoc-in [sock-id :atime] (cur-time))
        (case sl
          ":quit" (send open-sockets closeconn-message sock-id "Good bye. See you later.")
          ":terminate" (do 
                         (send open-sockets closeconn-message sock-id "Graceful shutdown will be initialized.")
                         (shutdown-server))
          (prn (perform-query s)))) 
      (catch SocketException e
        (send open-sockets closeconn-message sock-id "Closing by error. Sorry.")) 
      (catch Throwable t
        (log-t "Exception: " t) )
      (finally
        (send open-sockets closeconn-message sock-id "Finalization proceed.")))))


;; Hint: future, deliver
(defn- run-loop [server-sock]
  (try
    (let [^Socket sock (.accept server-sock)
          sock-id (keyword (gensym "client-socket"))]
      (send open-sockets assoc-in [sock-id] {:socket sock
                                             :atime (cur-time)})
      (future (handle-request sock sock-id)))
    (catch SocketException e)
    (catch Throwable t
      (log-t "" t)
      (shutdown-server))))

(defn check-open-sockets
  []
  (let [now (cur-time)
        inactive-sockets (remove (fn [[sock-id {socket :socket atime :atime}]]
                                   (< (- now atime) inactive-timeout)) 
                           @open-sockets)]
    (doseq [[socket-id _] inactive-sockets]
      (send open-sockets closeconn-message socket-id "Inactive session, killed."))))

(defn run [port]
  (binding [*should-be-finished* (promise)]
    (future
      (reset! server-socket (doto (ServerSocket.)
                              (.setReuseAddress true)
                              (.bind (InetSocketAddress. 
                                       "0.0.0.0" port)))) 
      (while (not (realized? *should-be-finished*))
        (run-loop @server-socket))
      (shutdown-server))
    (future
      (while (not (realized? *should-be-finished*))
        (check-open-sockets)
        (Thread/sleep 250)))))
