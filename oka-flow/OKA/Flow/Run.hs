{-# LANGUAGE MultiWayIf      #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE RecordWildCards #-}
-- |
-- Evaluator of dataflow graph.
module OKA.Flow.Run
  ( FlowCtx(..)
  , runFlow
  ) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM.TMVar
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.Operational
import Control.Monad.Trans.State.Strict
import Control.Monad.STM
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Map.Strict              ((!))
import Data.Set                     qualified as Set
import Data.Time                    (NominalDiffTime,getCurrentTime,diffUTCTime)
import Data.Typeable
import System.FilePath              ((</>))
import System.Directory             (createDirectory,createDirectoryIfMissing,renameDirectory,removeDirectoryRecursive)

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Types

----------------------------------------------------------------
--
----------------------------------------------------------------


-- | Structure which carry callbacks for logging of events during
--   execution
data FlowLogger = FlowLogger
  { init  :: [StorePath] -> [StorePath] -> IO ()
    -- | Called when we evaluation of workflow starts. First
    --   parameter is already existing output, second is one
    --   which we need to evaluate.
  , start :: StorePath -> IO ()
    -- | Called when job is started
  , done  :: StorePath -> NominalDiffTime -> IO ()
    -- | Called when job finishes execution
  , crash :: String -> IO ()
    -- | Called when job crashes
  }

instance Semigroup FlowLogger where
  a <> b = FlowLogger { init  = (liftA2 . liftA2) (>>) a.init b.init
                      , start = liftA2 (>>) a.start b.start
                      , done  = liftA2 (>>) a.done  b.done
                      , crash = liftA2 (>>) a.crash b.crash
                      }

instance Monoid FlowLogger where
  mempty = FlowLogger { init  = mempty
                      , start = mempty
                      , done  = mempty
                      , crash = mempty
                      }

-- | Evaluation context for dataflow program
data FlowCtx eff = FlowCtx
  { flowCtxRoot    :: FilePath
    -- ^ Root directory for cache
  , flowTgtExists  :: FilePath -> IO Bool
    -- ^ Function which is used to check whether output exists
  , flowCtxEff     :: forall a. eff a -> IO a
    -- ^ Evaluator for effects allowed in dataflow program
  , flowCtxRes     :: ResourceSet
    -- ^ Resources which are available for evaluator.
  , logger         :: FlowLogger
    -- ^ Logger for evaluator
  }


----------------------------------------------------------------
-- Execution
----------------------------------------------------------------

-- | Execute dataflow program
runFlow
  :: (ResultSet r)
  => FlowCtx eff -- ^ Evaluation context
  -> Metadata    -- ^ Metadata for program
  -> Flow eff r  -- ^ Dataflow program
  -> IO ()
runFlow ctx@FlowCtx{..} meta (Flow m) = do
  -- Evaluate dataflow graph
  gr <- fmap (deduplicateGraph . hashFlowGraph)
      $ interpretWithMonad flowCtxEff
      $ fmap (\(r,(_,gr)) -> addTargets r gr)
      $ runStateT m (meta, FlowGraph mempty mempty)
  -- Prepare graph for evaluation
  targets <- shakeFlowGraph targetExists gr
  gr_exe  <- addTMVars gr
  let getStorePath fids = concat [ gr ^.. flowGraphL . at fid . _Just . to (.output) . _Just
                                 | fid <- toList fids
                                 ]
      paths_wanted = getStorePath targets.wanted
      paths_exist  = getStorePath targets.exists
  ctx.logger.init paths_exist paths_wanted
  -- Evaluator
  mapConcurrently_ id
    [ prepareFun ctx gr_exe targets (gr_exe.graph ! i) flowCtxRes
    | i <- Set.toList targets.wanted
    ]
  where
    addTargets r gr = gr & flowTgtL %~ mappend (Set.fromList (toResultSet r))
    targetExists path = flowTgtExists (flowCtxRoot </> storePath path)


-- Add TMVars to each node of graph. This is needed in order to
addTMVars :: FlowGraph a -> IO (FlowGraph (TMVar (), a))
addTMVars = traverse $ \f -> do v <- newEmptyTMVarIO
                                pure (v,f)

-- Prepare function for evaluation
prepareFun
  :: FlowCtx eff                           -- Evaluation context
  -> FlowGraph (TMVar (), Maybe StorePath) -- Full dataflow graph
  -> FIDSet                                -- Our target set
  -> Fun FunID (TMVar (), Maybe StorePath) -- Function to evaluate
  -> ResourceSet
  -> IO ()
prepareFun ctx@FlowCtx{..} FlowGraph{graph=gr} FIDSet{..} fun res = crashReport $ do
  -- Check that all function parameters are already evaluated:
  for_ fun.param $ \fid -> when (fid `Set.notMember` exists) $
    atomically $ readTMVar (fst $ outputOf fid)
  -- Request resources
  atomically $ fun.requestRes res
  -- Run action
  case fun.workflow of
    -- Prepare normal action. We first create output directory and
    -- write everything there. After we're done we rename it.
    Workflow (Action _ act) -> prepareNormal (act res)
    -- Execute phony action. We don't need to bother with setting up output
    Phony    act            -> act res meta params
  -- Signal that we successfully completed execution
  atomically $ do
    putTMVar (fst fun.output) ()
    fun.releaseRes res
  where
    outputOf k = case gr ^. at k of
      Just f  -> f.output
      Nothing -> error "INTERNAL ERROR: inconsistent flow graph"
    --
    meta   = fun.metadata                        -- Metadata
    paramP = toPath . outputOf <$> fun.param     -- Parameters relative to store
    params = [ flowCtxRoot </> p | p <- paramP ] -- Parameters as real path
    --
    toPath (_,Just path) = storePath path
    toPath (_,Nothing  ) = error "INTERNAL ERROR: dependence on PHONY node"
    --
    prepareNormal action = do
      let path  = case fun.output of
            (_, Just p) -> p
            _           -> error "INTERNAL ERROR: phony node treated as normal"
      let out   = flowCtxRoot </> storePath path -- Output directory
          build = out ++ "-build"                -- Temporary build directory
      createDirectoryIfMissing False (flowCtxRoot </> path.name)
      createDirectory build
      BL.writeFile (build </> "meta.json") $ JSON.encode $ encodeMetadataDyn meta
      writeFile    (build </> "deps.txt")  $ unlines paramP
      t1 <- getCurrentTime
      ctx.logger.start path
      _  <- action meta params build `onException` removeDirectoryRecursive build
      t2 <- getCurrentTime
      ctx.logger.done path (diffUTCTime t2 t1)
      renameDirectory build out
    --
    crashReport = handle $ \(SomeException e) -> do
      let name = case fun.output of
            (_,Just p)  -> storePath p
            (_,Nothing) -> "<PHONY>"
      if | Just AsyncCancelled <- cast e -> pure ()
         | otherwise -> ctx.logger.crash $ "CRASHED: " ++ name ++ ": " ++ show e
      throwIO e
