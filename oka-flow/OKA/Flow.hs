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
  , Result
  , S(..)
  , ToS(..)
  , FlowArgument(..)
    -- ** Primitives
  , appendMeta
  , scopeMeta
  , withEmptyMeta
  , restrictMeta
  , want
  , liftEff
  , addExtMeta
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
import Effectful
import Effectful.State.Static.Local qualified as Eff

import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Flow
import OKA.Flow.Core.Run
import OKA.Flow.Core.Result
import OKA.Flow.Tools
import OKA.Flow.Core.Resources
import OKA.Flow.Core.S
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
  Eff.modify $ stMetaL %~ storeExternal @a fid
