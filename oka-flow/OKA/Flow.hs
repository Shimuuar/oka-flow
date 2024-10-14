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
  , appendMeta
  , scopeMeta
  , restrictMeta
  , Result
  , ResultSet(..)
  , want
  , liftEff
  , addExtMeta
    -- * Defining workflows
  , Workflow(..)
  , Action(..)
  , basicLiftWorkflow
  , liftWorkflow
  , basicLiftPhony
  , liftPhony
  , basicLiftExe
    -- * Resources
  , Resource(..)
  , ResAsMutex(..)
  , ResAsCounter(..)
    -- * Execution
  , FlowCtx(..)
  , FlowLogger(..)
  , runFlow
  ) where

import Control.Lens
import Effectful
import Effectful.State.Static.Local qualified as Eff

import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Graph
import OKA.Flow.Types
import OKA.Flow.Run
import OKA.Flow.Tools
import OKA.Flow.Resources
import OKA.Flow.Std


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
  Eff.modify $ stMetaL . hkdMetadata .~ Load @_ @a fid 


-- | Lift phony action using standard tools
liftPhony
  :: (Resource res, FlowArgument args)
  => res
     -- ^ Resources required by workflow
  -> (Metadata -> args -> IO ())
  -> AsRes args
  -> Flow eff ()
liftPhony res exe = basicLiftPhony res $ \_ meta args -> do
  a <- runFlowArguments args
  exe meta a
