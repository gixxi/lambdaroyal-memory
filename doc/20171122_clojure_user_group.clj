;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;   _________ .__             __                                       
;;   \_   ___ \|  |   ____    |__|__ _________   ____                   
;;   /    \  \/|  |  /  _ \   |  |  |  \_  __ \_/ __ \                  
;;   \     \___|  |_(  <_> )  |  |  |  /|  | \/\  ___/                  
;;    \______  /____/\____/\__|  |____/ |__|    \___  >                 
;;           \/           \______|                  \/                  
;;    ____ ___                       ________                           
;;   |    |   \______ ___________   /  _____/______  ____  __ ________  
;;   |    |   /  ___// __ \_  __ \ /   \  __\_  __ \/  _ \|  |  \____ \ 
;;   |    |  /\___ \\  ___/|  | \/ \    \_\  \  | \(  <_> )  |  /  |_> >
;;   |______//____  >\___  >__|     \______  /__|   \____/|____/|   __/ 
;;                \/     \/                \/                   |__|    
;;
;;   STM-based Database lambdaroyal-memory
;;   https://github.com/gixxi/lambdaroyal-memory
;;
;;   christian.meichsner@live.com - http://www.planet-rocklog.com
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(require 
 '[lambdaroyal.memory.ui-helper :refer :all]
 '[lambdaroyal.memory.core.tx :refer :all]
 '[lambdaroyal.memory.core.context :refer :all]
 '[lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric]]
 '[lambdaroyal.memory.helper :refer :all])
(import [lambdaroyal.memory.core ConstraintException])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; slurp-in some demodata
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def articles (slurp-csv "doc/resources/articles.csv" 
                         :ident string-to-string	
                         :client string-to-string	
                         :unit string-to-string
                         :price string-to-float 
                         :inboundAmount string-to-long))

(def stocks (slurp-csv "doc/resources/stocks.csv"
                       :article string-to-long	
                       :amount string-to-float
                       :batch string-to-string	
                       :color string-to-string
                       :size string-to-string))

