{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ImportQualifiedPost #-}
module OKA.Flow.Graph where

import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Exception
import Data.Foldable
import System.FilePath ((</>))
import Data.Map.Strict qualified as Map
import Data.Map.Strict   (Map, (!))
import Data.Set        qualified as Set
import Data.Set          (Set)
import Control.Monad.STM
import Control.Concurrent.Async
import OKA.Metadata

type Meta = Metadata


----------------------------------------------------------------
-- Data types
----------------------------------------------------------------

-- | Internal identifier of dataflow function in complete dataflow
--   graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

data Action res
  = ActNormal (res -> STM (Meta -> [FilePath] -> FilePath -> IO ()))
  | ActPhony  (res -> STM (Meta -> [FilePath] -> IO ()))
  

-- | Descritpion of workflow function. It knows how to build 
data Workflow res = Workflow
  { workflowRun    :: Action res
    -- ^ Start workflow. This function takes resources as input and
    --   return STM action which either returns actual IO function to
    --   run or retries if it's unable to secure necessary resources.
  , workflowName   :: String
    -- ^ Name of workflow. Used for caching
  }

isPhony :: Workflow res -> Bool
isPhony f = case workflowRun f of
  ActNormal{} -> False
  ActPhony{}  -> True

-- | Single workflow bound in dataflow graph.
data Fun res a = Fun
  { funWorkflow :: Workflow res
    -- ^ Execute workflow. It takes resource handle as parameter and
    --   tries to acquire resources and return actual function to run.
  , funMetadata :: Meta
    -- ^ Metadata that should be supplied to the workflow
  , funParam    :: [a]
    -- ^ Parameters to workflow. They're resolved to
  , funOutput   :: a
  }
  deriving stock (Functor)

-- | Complete dataflow graph
data FlowGraph res = FlowGraph
  { flowGraph :: Map FunID (Fun res FunID)
    -- ^ Dataflow graph with dependencies
  , flowTgt   :: Set FunID
    -- ^ Set of values for which we want to evaluate
  }

flowTgtL :: Lens' (FlowGraph res) (Set FunID)
flowTgtL = lens flowTgt (\x s -> x {flowTgt = s})

flowGraphL :: Lens' (FlowGraph res) (Map FunID (Fun res FunID))
flowGraphL = lens flowGraph (\x s -> x {flowGraph = s})

----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

newtype Hash = Hash String

data StorePath = StorePath String Hash

storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm ++ "-" ++ hash

hashMeta :: Meta -> Hash
hashMeta = undefined

hashFun :: Fun res (FunID, StorePath) -> StorePath
hashFun f = undefined



hashFlowGraph
  :: Map FunID (Fun res FunID)
  -> Map FunID (Fun res (FunID, StorePath))
hashFlowGraph m = r where
  r = fmap convertFun m
  convertFun f = f' where
    f' = f { funOutput = ( funOutput f, hashFun f')
           , funParam  = [ (i, snd $ funOutput $ r ! i) | i <- funParam f ]
           }

-- | Remove all 
shakeGraph
  :: (Monad m)
  => (StorePath -> m Bool)                  -- ^ Predicate to check whether
  -> Set FunID                              -- ^ Set of targets
  -> Map FunID (Fun res (FunID, StorePath)) -- ^ Workflow graph
  -> m FIDSet
shakeGraph tgtExists targets workflows
  = foldM addFID (FIDSet mempty mempty) targets
  where
    addFID fids@FIDSet{..} fid
      -- We already wisited these workflows
      | fid `Set.member` fidExists = pure fids
      | fid `Set.member` fidWanted = pure fids
      -- Workflow does not have target. We will have to execute it.
      | isPhony (funWorkflow f)    = pure fids'
      -- Check if resul has been cached already
      | otherwise = tgtExists (snd (funOutput f)) >>= \case
          True  -> pure $ fids & fidExistsL %~ Set.insert fid
          False -> foldM addFID fids' (fst <$> funParam f)
      where
        f     = workflows ! fid
        fids' = fids & fidWantedL %~ Set.insert fid
        deps  = funParam f

data FIDSet = FIDSet
  { fidExists :: !(Set FunID)
  , fidWanted :: !(Set FunID)
  }

fidExistsL, fidWantedL :: Lens' FIDSet (Set FunID)
fidExistsL = lens fidExists (\f x -> f {fidExists = x})
fidWantedL = lens fidWanted (\f x -> f {fidWanted = x})


----------------------------------------------------------------
-- Execution
----------------------------------------------------------------

executeWorkflow
  :: FilePath
  -> res
  -> Map FunID (Fun res (FunID, StorePath))
  -> FIDSet
  -> IO ()
executeWorkflow root res workflows FIDSet{..} = go [] (toList fidWanted) where
  go []     []    = pure ()
  go asyncs tasks = do
    res <- atomically $ asum
      [ CmdRunTask <$> pickSTM useFun tasks
      ]
    case res of
      CmdRunTask  (tasks',  io)      -> withAsync io $ \a -> go (a:asyncs) tasks'
      CmdTaskDone (asyncs', Nothing) -> go asyncs' tasks
      CmdTaskDone (_      , Just e)  -> throwIO e
      CmdNothingToDo                 -> error "Deadlock"
  --
  useFun fid = case workflowRun $ funWorkflow fun of
    ActNormal run -> (\fun -> fun meta param out) <$> run res
    ActPhony  run -> (\fun -> fun meta param)     <$> run res
    where
      fun = workflows ! fid
      meta = funMetadata fun
      param = [root </> storePath path | (_,path) <- funParam fun]
      out   = root </> storePath (snd (funOutput fun))

data Cmd
  = CmdRunTask  ([FunID],    IO ())
  | CmdTaskDone ([Async ()], Maybe SomeException)
  | CmdNothingToDo

pickSTM :: (a -> STM b) -> [a] -> STM ([a], b)
pickSTM fun = go id where
  go asF []     = retry
  go asF (a:as) =  do b <- fun a
                      pure (asF as, b)
               <|> go (asF . (a:)) as
