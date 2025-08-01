{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE RecordWildCards #-}
-- |
-- Evaluator of dataflow graph.
module OKA.Flow.Run
  ( FlowCtx(..)
  , FlowLogger(..)
  , runFlow
  ) where

import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Exception
import Control.Lens
import Control.Monad.STM
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Map.Strict              (Map,(!))
import Data.Map.Strict              qualified as Map
import Data.Monoid                  (Endo(..))
import Data.Set                     qualified as Set
import Data.Time                    (NominalDiffTime,getCurrentTime,diffUTCTime)
import Data.Typeable
import Data.Void
import Effectful
import Effectful.State.Static.Local qualified as Eff

import System.FilePath              ((</>))
import System.Directory             (createDirectory,createDirectoryIfMissing,renameDirectory,removeDirectoryRecursive,
                                     doesDirectoryExist
                                    )

import OKA.Metadata
import OKA.Metadata.Meta
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
  , runEffect :: forall a. Eff eff a -> Eff '[IOE] a
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
      $ runEff
      $ runEffect
      $ fmap (\(r,st) -> addTargets r st.graph)
      $ Eff.runState FlowSt{ meta  = absurd <$> meta
                           , graph = FlowGraph mempty mempty
                           }
      $ m
  -- Prepare graph for evaluation
  targets <- shakeFlowGraph targetExists gr
  gr_exe  <- addMVars targets gr
  let getStorePath fids = concat [ gr ^.. flowGraphL . at fid . _Just . to (.output) . _Just
                                 | fid <- toList fids
                                 ]
  ctx.logger.init (getStorePath targets.exists)
                  (getStorePath targets.wanted)
  -- Prepare dictionary of external metadata which is loaded from
  -- store path
  ext_meta <- prepareExtMetaCache ctx gr_exe targets
  -- Evaluator
  mapConcurrently_ id
    [ prepareFun ctx gr_exe ext_meta (gr_exe.graph ! i)
    | i <- Set.toList targets.wanted
    ]
  where
    addTargets r = flowTgtL %~ mappend (Set.fromList (toResultSet r))
    targetExists path = doesDirectoryExist (ctx.root </> storePath path)


-- Add MVars to each node of graph. They are used to block evaluator
-- until all dependencies are ready
addMVars :: FIDSet -> FlowGraph a -> IO (FlowGraph (MVar (), a))
addMVars FIDSet{exists}
  = traverseOf flowGraphL
  $ Map.traverseWithKey $ \fid fun -> do
      v <- if | fid `Set.member` exists -> newMVar ()
              | otherwise               -> newEmptyMVar
      pure ((v,) <$> fun)

-- Prepare functixon for evaluation
prepareFun
  :: FlowCtx eff                          -- Evaluation context
  -> FlowGraph (MVar (), Maybe StorePath) -- Full dataflow graph
  -> ExtMetaCache                         -- External metadata
  -> Fun FunID (MVar (), Maybe StorePath) -- Function to evaluate
  -> IO ()
prepareFun ctx FlowGraph{graph=gr} ext_meta fun = crashReport ctx.logger fun $ do
  -- Check that all function parameters are already evaluated:
  for_ fun.param $ \fid -> readMVar (fst $ outputOf fid)
  -- Compute metadata which should be passed to the workflow by
  -- applying data loaded from
  meta <- traverseMetadata (lookupExtCache ext_meta) fun.metadata
  -- Request resources
  atomically $ fun.resources.acquire ctx.res
  -- Run action
  case fun.workflow of
    -- Prepare normal action. We first create output directory and
    -- write everything there. After we're done we rename it.
    Workflow (Action _ act) -> prepareNormal meta (act ctx.res)
    WorkflowExe exe         -> prepareExe    meta exe
    -- Execute phony action. We don't need to bother with setting up output
    Phony    act            -> act.run ctx.res meta params
  -- Signal that we successfully completed execution
  putMVar (fst fun.output) ()
  atomically $ fun.resources.release ctx.res
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
    -- Execution of an external executable
    prepareExe meta exe@Executable{} = normalExecution meta $ \build -> do
      exe.io ctx.res
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
  if | Just AsyncCancelled <- cast e
       -> pure ()
     | Just (SomeAsyncException e') <- cast e
     , Just AsyncCancelled          <- cast e'
       -> pure ()
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



----------------------------------------------------------------
-- Cache for external metadata. We only want to load data once
----------------------------------------------------------------


newtype ExtMetaCache = ExtMetaCache (Map (TypeRep, FunID) SomeExtMeta)

data SomeExtMeta where
  SomeExtMeta :: IsMetaPrim a => IO a -> SomeExtMeta

lookupExtCache :: forall a. Typeable a => ExtMetaCache -> FunID -> IO a
lookupExtCache (ExtMetaCache cache) fid = 
  case Map.lookup (ty, fid) cache of
    Nothing -> error "INTERNAL ERROR: missing entry in cache"
    Just (SomeExtMeta io) -> do
      a <- io
      case cast a of
        Nothing -> error "INTERNAL ERROR: corrupted ext_meta cache"
        Just a' -> pure a'
  where
    ty = typeRep (Proxy @a)


prepareExtMetaCache
  :: FlowCtx eff
  -> FlowGraph (MVar (), Maybe StorePath)
  -> FIDSet
  -> IO ExtMetaCache
prepareExtMetaCache ctx gr targets = do
  cache <- traverse (\(SomeExtMeta io) -> SomeExtMeta <$> once io)
         $ Map.fromList
         $ flip appEndo []
         $ foldMap
           (\fun -> getConst $ traverseMetadata toCache fun.metadata)
           gr.graph
  pure $ ExtMetaCache cache
  where
    toCache :: forall a. IsMetaPrim a => FunID -> Const (Endo [((TypeRep, FunID), SomeExtMeta)]) a
    toCache fid
      = Const
      $ Endo
      ( ( (typeRep (Proxy @a), fid)
        , SomeExtMeta $ if
            | fid `Set.member` targets.wanted ->
                 do readMVar (fst $ (gr.graph ! fid).output)
                    readMeta @a path
            | otherwise ->
                do readMeta @a path
        )
      :)
      where
        path = case (gr.graph ! fid).output of
          (_,Just p)  -> ctx.root </> storePath p </> "saved.json"
          (_,Nothing) -> error "Dependence on PHONY target"
    --
    readMeta :: IsMetaPrim a => FilePath -> IO a
    readMeta path = do
      (JSON.eitherDecode <$> BL.readFile path) >>= \case
        Left  e  -> error e
        Right js -> case decodeMetadataPrimEither js of
          Left  e -> throw e
          Right a -> pure a

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
