(ns paperboy.support.mqtt-broker
  (:import [io.moquette.broker Server]
           [io.moquette.broker.config MemoryConfig]
           [java.util Properties]))

(defn broker
  [port]
    {:server (Server.)
     :host "127.0.0.1"
     :port port})

(defn start!
  [{:keys [^Server server host port]}]
  (let [properties
        (doto (Properties.)
          (.setProperty "host" host)
          (.setProperty "port" (str port))
          (.setProperty "allow_anonymous" "true")
          (.setProperty "persistence_enabled" "false"))]
    (.startServer server (MemoryConfig. properties))))

(defn stop!
  [{:keys [^Server server]}]
  (.stopServer server))