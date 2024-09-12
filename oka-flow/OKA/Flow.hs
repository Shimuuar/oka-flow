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
  , appendMeta
  , scopeMeta
  , restrictMeta
  , Result
  , ResultSet(..)
  , want
  , liftEff
  , withExtMeta
  , withoutExtMeta
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

import Control.Monad.State.Strict
import Control.Monad.Reader
import Control.Monad.Operational
import Data.Typeable

import OKA.Metadata
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
liftEff :: eff a -> Flow eff a
liftEff = Flow . lift . lift . singleton

-- | Load contents of saved meta before execution of workflow
withExtMeta
  :: forall a eff b. IsMeta a
  => Result (SavedMeta a) -> Flow eff b -> Flow eff b
withExtMeta (Result fid) (Flow action) = Flow $ local (ext:) action
  where
    ext = ExtMeta { key = fid
                  , tyRep = typeOf (undefined :: a)
                  , load  = toMetadata . decodeMetadata @a
                  }

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
