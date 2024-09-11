{-# LANGUAGE RecordWildCards  #-}
-- |
-- Evaluator of dataflow graph.
module OKA.Flow.Run
  ( FlowCtx(..)
  , FlowLogger(..)
  , runFlow
  ) where

import Control.Concurrent.Async
import Control.Concurrent.STM.TMVar
import Control.Concurrent.MVar
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.Operational
import Control.Monad.Trans.State.Strict
import Control.Monad.STM
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Traversable
import Data.Map.Strict              ((!))
import Data.Map.Strict              qualified as Map
import Data.Set                     qualified as Set
import Data.Time                    (NominalDiffTime,getCurrentTime,diffUTCTime)
import Data.Typeable
import System.FilePath              ((</>))
import System.Directory             (createDirectory,createDirectoryIfMissing,renameDirectory,removeDirectoryRecursive,
                                     doesDirectoryExist
                                    )

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Resources
import OKA.Flow.Types
import OKA.Flow.Tools



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
  , crash :: Maybe StorePath -> SomeException -> IO ()
    -- | Called when job crashes
  }

instance Semigroup FlowLogger where
  a <> b = FlowLogger { init  = a.init  <> b.init
                      , start = a.start <> b.start
                      , done  = a.done  <> b.done
                      , crash = a.crash <> b.crash
                      }

instance Monoid FlowLogger where
  mempty = FlowLogger { init  = mempty
                      , start = mempty
                      , done  = mempty
                      , crash = mempty
                      }

-- | Evaluation context for dataflow program
data FlowCtx eff = FlowCtx
  { root      :: FilePath
    -- ^ Root directory for cache
  , runEffect :: forall a. eff a -> IO a
    -- ^ Evaluator for effects allowed in dataflow program
  , res       :: ResourceSet
    -- ^ Resources which are available for evaluator.
  , logger    :: FlowLogger
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
runFlow ctx@FlowCtx{runEffect} meta (Flow m) = do
  -- Evaluate dataflow graph
  gr <- fmap (deduplicateGraph . hashFlowGraph)
      $ interpretWithMonad runEffect
      $ fmap (\(r,st) -> addTargets r st.graph)
      $ runStateT m FlowSt{ meta       = meta
                          , graph      = FlowGraph mempty mempty
                          , transforms = []
                          }
  -- Prepare graph for evaluation
  targets <- shakeFlowGraph targetExists gr
  gr_exe  <- addTMVars gr
  let getStorePath fids = concat [ gr ^.. flowGraphL . at fid . _Just . to (.output) . _Just
                                 | fid <- toList fids
                                 ]
  ctx.logger.init (getStorePath targets.exists)
                  (getStorePath targets.wanted)
  -- Prepare dictionary of external metadata which is loaded from
  -- store path
  ext_meta
    <- fmap Map.fromList
    $  sequence
    [ case k `Set.member` targets.wanted of
        True  -> do io <- once readMeta
                    pure (k, do atomically $ readTMVar (fst $ (gr_exe.graph ! k).output)
                                io
                         )
        False -> do io <- once readMeta
                    pure (k,io)
      --
    | Fun{extMeta} <- toList gr_exe.graph
    , ext          <- extMeta
    , let k    = ext.key
          path = case (gr_exe.graph ! k).output of
            (_,Just p)  -> ctx.root </> storePath p </> "saved.json"
            (_,Nothing) -> error "Dependence on PHONY target"
          readMeta = do
            (JSON.eitherDecode <$> BL.readFile path) >>= \case
              Left  e  -> error e
              Right js -> pure $ ext.load js
    ]
  -- Evaluator
  mapConcurrently_ id
    [ prepareFun ctx gr_exe targets ext_meta (gr_exe.graph ! i) ctx.res
    | i <- Set.toList targets.wanted
    ]
  where
    addTargets r gr = gr & flowTgtL %~ mappend (Set.fromList (toResultSet r))
    targetExists path = doesDirectoryExist (ctx.root </> storePath path)


-- Add TMVars to each node of graph. This is needed in order to
addTMVars :: FlowGraph a -> IO (FlowGraph (TMVar (), a))
addTMVars = traverse $ \f -> do v <- newEmptyTMVarIO
                                pure (v,f)

-- Prepare function for evaluation
prepareFun
  :: FlowCtx eff                           -- Evaluation context
  -> FlowGraph (TMVar (), Maybe StorePath) -- Full dataflow graph
  -> FIDSet                                -- Our target set
  -> Map.Map FunID (IO Metadata)
  -> Fun FunID (TMVar (), Maybe StorePath) -- Function to evaluate
  -> ResourceSet
  -> IO ()
prepareFun ctx FlowGraph{graph=gr} FIDSet{..} ext_meta fun res = crashReport ctx.logger fun $ do
  -- Check that all function parameters are already evaluated:
  for_ fun.param $ \fid -> when (fid `Set.notMember` exists) $
    atomically $ readTMVar (fst $ outputOf fid)
  -- Compute metadata which should be passed to the workflow by
  -- applying data loaded from 
  meta <- do
    ext <- for fun.extMeta $ \ExtMeta{key} -> ext_meta ! key
    pure $! foldr (flip (<>)) fun.metadata ext
  -- Request resources
  atomically $ fun.resources.acquire res
  -- Run action
  case fun.workflow of
    -- Prepare normal action. We first create output directory and
    -- write everything there. After we're done we rename it.
    Workflow (Action _ act) -> prepareNormal meta (act res)
    WorkflowExe exe         -> prepareExe    meta exe
    -- Execute phony action. We don't need to bother with setting up output
    Phony    act            -> act res meta params
  -- Signal that we successfully completed execution
  atomically $ do
    putTMVar (fst fun.output) ()
    fun.resources.release res
  where
    outputOf k = case gr ^. at k of
      Just f  -> f.output
      Nothing -> error "INTERNAL ERROR: inconsistent flow graph"
    --
    paramP = toPath . outputOf <$> fun.param  -- Parameters relative to store
    params = [ ctx.root </> p | p <- paramP ] -- Parameters as real path
    --
    toPath (_,Just path) = storePath path
    toPath (_,Nothing  ) = error "INTERNAL ERROR: dependence on PHONY node"
    -- Execution of haskell action
    prepareNormal meta action = normalExecution meta $ \build -> do
      action meta params build
    -- Execution of an extenrnal executable
    prepareExe meta exe@Executable{} = normalExecution meta $ \build -> do
      exe.io res
      runExternalProcess exe.executable meta (build:params)
    -- Standard wrapper for execution of workflows that create outputs
    normalExecution meta action = do
      let path  = case fun.output of
            (_, Just p) -> p
            _           -> error "INTERNAL ERROR: phony node treated as normal"
      ctx.logger.start path
      t1 <- getCurrentTime
      withBuildDirectory ctx.root path $ \build -> do
        BL.writeFile (build </> "meta.json") $ JSON.encode $ encodeMetadata meta
        writeFile    (build </> "deps.txt")  $ unlines paramP
        () <- action build
        pure ()
      t2 <- getCurrentTime
      ctx.logger.done path (diffUTCTime t2 t1)



-- Report crash in case of exception
crashReport :: FlowLogger -> Fun i (a, Maybe StorePath) -> IO x -> IO x
crashReport logger fun = handle $ \e0@(SomeException e) -> do
  let path = snd fun.output
  if | Just AsyncCancelled <- cast e -> pure ()
     | otherwise -> logger.crash path e0
  throwIO e

-- Create directory with -build prefix and rename it to final name
withBuildDirectory :: FilePath -> StorePath -> (FilePath -> IO a) -> IO a
withBuildDirectory root path action = do
  createDirectoryIfMissing False group
  createDirectory build
  a <- action build `onException` removeDirectoryRecursive build
  a <$ renameDirectory build out
  where
    group = root </> path.name
    out   = root </> storePath path
    build = out <> "-build"


-- | Return action that will perform action exactly once
once :: IO a -> IO (IO a)
once action = do
  cache <- newMVar Nothing
  return $ readMVar cache >>= \case
    Just a  -> pure a
    Nothing -> modifyMVar cache $ \case
      Just a  -> return (Just a, a)
      Nothing -> do a <- action
                    return (Just a, a)
