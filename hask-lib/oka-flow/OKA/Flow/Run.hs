{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
-- |
module OKA.Flow.Run
  ( executeWorkflow
  ) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM.TMVar
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.STM
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.ByteString              (ByteString)
import Data.ByteString.Lazy         qualified as BL
import Data.ByteString.Base16       qualified as Base16
import Data.ByteString.Builder      qualified as BB
import Data.ByteString.Char8        qualified as BC8
import Data.Foldable
import Data.HashMap.Strict          qualified as HM
import Data.List                    (sortOn)
import Data.Map.Strict              (Map, (!))
import Data.Map.Strict              qualified as Map
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import Data.Vector                  qualified as V
import System.FilePath              ((</>))

import OKA.Metadata
import OKA.Flow.Graph


----------------------------------------------------------------
-- Execution
----------------------------------------------------------------

executeWorkflow
  :: FilePath
  -> res
  -> Map FunID (Fun res (FunID, StorePath))
  -> FIDSet
  -> IO ()
executeWorkflow root res workflows FIDSet{..} = do
  ready <- traverse (const newEmptyTMVarIO) workflows
  go ready [] (toList fidWanted)
  where
    go _     []     []    = pure ()
    go ready asyncs tasks = do
      r <- atomically $ asum
        [ CmdRunTask  <$> pickSTM (useFun ready) tasks
        , CmdTaskDone <$> pickSTM waitCatchSTM   asyncs
        ]
      case r of
        CmdRunTask  (tasks',  io)        -> withAsync io $ \a -> go ready (a:asyncs) tasks'
        CmdTaskDone (asyncs', Right fid) -> do atomically $ putTMVar (ready ! fid) ()
                                               go ready asyncs' tasks
        CmdTaskDone (_      , Left  e)   -> throwIO e
        CmdNothingToDo                   -> error "Deadlock"                                           
    --
    useFun ready fid = paramOK >> case workflowRun $ funWorkflow fun of
      ActNormal run -> (\f -> fid <$ f meta param out) <$> run res
      ActPhony  run -> (\f -> fid <$ f meta param)     <$> run res
      where
        fun     = workflows ! fid
        meta    = funMetadata fun
        param   = [root </> storePath path | (_,path) <- funParam fun]
        paramOK = sequence_ [ readTMVar (ready ! i) | (i,_) <- funParam fun]
        out     = root </> storePath (snd (funOutput fun))

data Cmd
  = CmdRunTask  ([FunID],       IO FunID)
  | CmdTaskDone ([Async FunID], Either SomeException FunID)
  | CmdNothingToDo

pickSTM :: (a -> STM b) -> [a] -> STM ([a], b)
pickSTM fun = go id where
  go _   []     = retry
  go asF (a:as) =  do b <- fun a
                      pure (asF as, b)
               <|> go (asF . (a:)) as
