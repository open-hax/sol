(ns open-hax.sol.extern.console
  "Host console boundary. Layers above extern log through these functions
   instead of touching `js/console` directly.")

(defn error!
  "Write a structured error line to the host console."
  [message data]
  (.error js/console message data))
