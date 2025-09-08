-- |
-- Simple framework for dataflow programming for data
-- analysis. Primary target is OKA experiment.
--
-- It uses two-stage evaluation. There's computation with metadata and
-- building of dependency graph that's assumed to be fast and
-- execution of dataflow stages which are long lasting and produce
-- their results as data in file system. This is needed for caching.
--
-- Spawning workers as separate processes is needed in order to solve
-- following problems: 1) we need to mix code written in different
-- languages 2) do something about long compile times of haskell
-- programs. Latter is solved by having many small programs instead
-- one large one. This way we can recompile only few modules at time.
module OKA.Flow
  ( -- * Flow monad
    Flow
  , MetadataFlow
  , lookupMeta
  , appendMeta
  , scopeMeta
  , withEmptyMeta
  , restrictMeta
  , Result
  , ToS(..)
  , want
  , liftEff
  , addExtMeta
    -- * Defining workflows
  , Workflow(..)
  , Action(..)
  , basicLiftWorkflow
  , liftWorkflow
  , basicLiftPhony
  -- , liftPhony
  , basicLiftExe
    -- * Resources
  , ResourceClaim(..)
  , ResourceDef(..)
  , ResAsMutex(..)
  , ResAsCounter(..)
    -- * Execution
  , FlowCtx(..)
  , FlowLogger(..)
  , runFlow
  ) where

import Control.Lens
import Control.Monad.State
import Effectful
import Effectful.State.Static.Local qualified as Eff

import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Flow
import OKA.Flow.Core.Run
-- import OKA.Flow.Tools
import OKA.Flow.Core.Resources
import OKA.Flow.Core.S
import OKA.Flow.Std
import OKA.Flow.Internal.Util

----------------------------------------------------------------
-- Flow monad
----------------------------------------------------------------

-- | Lift effect
liftEff :: Eff eff a -> Flow eff a
liftEff = Flow . raise

-- | Load contents of saved meta before execution of workflow
addExtMeta
  :: forall a eff. IsMeta a
  => Result (SavedMeta a) -> Flow eff ()
addExtMeta (Result fid) = Flow $ do
  Eff.modify $ stMetaL %~ storeExternal @a fid


-- -- | Lift phony action using standard tools
-- liftPhony
--   :: (ResourceClaim res, FlowArgument args)
--   => res
--      -- ^ Resources required by workflow
--   -> (Metadata -> args -> IO ())
--   -> AsRes args
--   -> Flow eff ()
-- liftPhony res exe = basicLiftPhony res $ \_ meta args -> do
--   a <- runFlowArguments args
--   exe meta a

-- | Lookup metadata. Unlike 'metadata' lens. This function only
--   requires 'IsFromMeta' and not full 'IsMeta'.
lookupMeta :: forall a eff. (IsMeta a) => Flow eff a
lookupMeta = do
  meta <- get
  case runMetaParserWith parseMetadata failK meta of
    Right Nothing  -> error $ "Failed to look up type " ++ typeName @a
    Right (Just a) -> pure a
    Left  e        -> error e
  where
    failK :: forall x k. IsMetaPrim x => k -> Either String (Maybe x)
    failK _ = Left $ "Encountered external metadata for "++typeName @a++". Cannot load"

