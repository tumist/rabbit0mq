{-# LANGUAGE OverloadedStrings, RankNTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Data.Text                        (Text)
import Data.Monoid                      ((<>))
import Control.Monad                    ((<=<), liftM, void, forever, forM_)
import Control.Exception                (bracket)
import Control.Concurrent               (myThreadId, killThread, threadDelay)
import System.Environment               (getEnvironment, getArgs)
import System.IO                        (BufferMode(LineBuffering), hSetBuffering, stdout)
import qualified Data.ByteString        as B
import qualified Data.ByteString.Char8  as C
import qualified Data.ByteString.Lazy   as LB
import qualified Data.Text              as T
import qualified Data.Text.Encoding     as E
import qualified Network.AMQP           as A
import qualified System.ZMQ3            as Z
import System.Log.Logger
import Network.AMQP
import System.ZMQ3.Monadic
{- import Control.Concurrent (threadDelay) -}

--------------------------------------------------------------------------------
-- zmq api
--------------------------------------------------------------------------------
data ZMQConnectInfo = ZMQConnectInfo String [B.ByteString] deriving Show

defaultZMQConnectInfo :: ZMQConnectInfo
defaultZMQConnectInfo = ZMQConnectInfo "tcp://127.0.0.1:7006" []

setZMQTopics :: ZMQConnectInfo -> [B.ByteString] -> ZMQConnectInfo
setZMQTopics (ZMQConnectInfo h _) ts = ZMQConnectInfo h ts

withMessage :: Receiver t => Socket z t -> ([B.ByteString] -> ZMQ z a) -> ZMQ z a
withMessage s cb = receiveMulti s >>= cb

subscribeTopics :: Subscriber t => Socket z t -> [B.ByteString] -> ZMQ z ()
subscribeTopics s [] = subscribe s B.empty
subscribeTopics s ts = mapM_ (subscribe s) ts

bindSocket' :: Z.Socket Pub -> ZMQConnectInfo -> IO ()
bindSocket' s (ZMQConnectInfo h _)= do
   infoM "rabbit0mq.connect-socket" "ZMQ conntected"
   Z.bind s h

bindSocket :: ZMQConnectInfo -> ZMQ z (Socket z Sub)
bindSocket (ZMQConnectInfo h ts) = do
   s <- socket Sub
   liftIO $ infoM "rabbit0mq.bind-socket" ("ZMQ topics " <> show ts)
   subscribeTopics s ts
   bind s h
   return s

sendZMQ :: Z.Socket Pub -> (Message, Envelope) -> IO ()
sendZMQ s (msg, env) = do
   infoM "rabbit0mq.send-zmq" (T.unpack routingKey)
   Z.send s [Z.SendMore] (E.encodeUtf8 routingKey) 
   Z.send' s [] (msgBody msg) --  TODO 
   where 
      routingKey = envRoutingKey env

--------------------------------------------------------------------------------
-- rabbitmq api
--------------------------------------------------------------------------------
data RabbitConnectInfo = RabbitConnectInfo String Text Text Text deriving Show

defaultRabbitConnectInfo :: RabbitConnectInfo
defaultRabbitConnectInfo = RabbitConnectInfo "127.0.0.1" "guest" "guest" "/"

eventExchangeName :: Text
eventExchangeName = "pmo.events"

sendRabbit :: A.Channel -> Text -> [B.ByteString] -> IO ()
sendRabbit _ _ [] = return ()
sendRabbit ch key (topic:messages) = do
   infoM "rabbit0mq.send-rabbit" (C.unpack topic)
   publishMsg ch key ("spud-webb." <> E.decodeUtf8 topic) (toMsg messages)
   where 
      toMsg bs = newMsg { msgBody = concatBS bs }
      concatBS = LB.concat . map fromStrict
   
withChannel :: RabbitConnectInfo -> (A.Channel -> IO a) -> IO a
withChannel (RabbitConnectInfo host user pass vhost) io = 
   bracket
      (openConnection host vhost user pass >>= addKillThreadOnCloseHandler)
      closeConnection
      (io <=< openChannel)

addKillThreadOnCloseHandler :: Connection -> IO Connection
addKillThreadOnCloseHandler conn = do
   tid <- myThreadId
   addConnectionClosedHandler conn True (killThread tid >> error "Connection closed") 
   return conn

configChannel :: A.Channel -> IO ()
configChannel ch = declareExchange ch newExchange {exchangeName = eventExchangeName, exchangeType = "topic"}

configChannel' :: [Text] -> A.Channel -> IO Text
configChannel' topics ch = do
   liftIO $ infoM "rabbit0mq.config-channel'" ("Rabbit topic(s) " <> show topics)
   (name, _, _) <- declareQueue ch newQueue {queueExclusive = True, queueAutoDelete = True} 
   declareExchange ch newExchange {exchangeName = eventExchangeName, exchangeType = "topic"}
   forM_ topics $ \topic -> bindQueue ch name eventExchangeName topic
   return name


--------------------------------------------------------------------------------
-- environment api
--------------------------------------------------------------------------------
getVars :: (String -> a -> String -> a) -> a -> [(String, String)] -> a
getVars f = foldl (\a' (k, v) -> f k a' v)

rabbitVars :: String -> RabbitConnectInfo -> String -> RabbitConnectInfo
rabbitVars "RABBITHOST" (RabbitConnectInfo _ u p v) h = RabbitConnectInfo h u p v
rabbitVars "RABBITUSER" (RabbitConnectInfo h _ p v) u = RabbitConnectInfo h (T.pack u) p v
rabbitVars "RABBITPASS" (RabbitConnectInfo h u _ v) p = RabbitConnectInfo h u (T.pack p) v
rabbitVars "RABBITVHOST" (RabbitConnectInfo h u p _) v = RabbitConnectInfo h u p (T.pack v)
rabbitVars _ inf _ = inf

zmqVars :: String -> ZMQConnectInfo -> String -> ZMQConnectInfo
zmqVars "ZMQHOST" (ZMQConnectInfo _ ts) h = ZMQConnectInfo h ts
zmqVars _ inf _ = inf

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------
main :: IO ()
main = do
   hSetBuffering stdout LineBuffering
   updateGlobalLogger rootLoggerName (setLevel INFO)
   noticeM "rabbit0mq.main" "Program starting"
   env <- getEnvironment
   let reversed = maybe False (const True) $ lookup "REVERSE" env
   let renv = getVars rabbitVars defaultRabbitConnectInfo env
   debugM "rabbit0mq.main" (show renv)
   zenv <- liftM (setZMQTopics (getVars zmqVars defaultZMQConnectInfo env) . map C.pack) getArgs
   debugM "rabbit0mq.main" (show zenv)
   if reversed
      then rtoz zenv renv
      else ztor zenv renv

rtoz :: ZMQConnectInfo -> RabbitConnectInfo -> IO ()
rtoz zenv@(ZMQConnectInfo _ ts) renv = withChannel renv $ \ch -> do
   qname <- configChannel' (map E.decodeUtf8 ts) ch 
   infoM "rabbit0mq.main" "RabbitMQ connected (reversed)"
   Z.withContext $ \ctx -> do   
      Z.withSocket ctx Z.Pub $ \s -> do
         bindSocket' s zenv
         liftIO $ infoM "rabbit0mq.rtoz" "ZeroMQ bound"
         void . liftIO $ consumeMsgs ch qname NoAck (sendZMQ s)
         forever $ threadDelay 60000000

ztor :: ZMQConnectInfo -> RabbitConnectInfo -> IO ()
ztor zenv renv = withChannel renv $ \ch -> do
   configChannel ch
   infoM "rabbit0mq.main" "RabbitMQ connected" 
   runZMQ $ do
      s <- bindSocket zenv 
      liftIO $ infoM "rabbit0mq.ztor" "ZeroMQ bound"
      forever . withMessage s $ liftIO . sendRabbit ch eventExchangeName

fromStrict :: B.ByteString -> LB.ByteString
fromStrict = LB.pack . B.unpack
