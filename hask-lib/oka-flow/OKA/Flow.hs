{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost        #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE TupleSections              #-}
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
  ( -- * Primitives
    Workflow(..)
  , Action(..)
    -- * Flow monad
  , Flow
  , appendMeta
  , modifyMeta
  , scopeMeta
  , restrictMeta
  , projectMeta
  , Result
  , ResultSet(..)
  , want
  , liftWorkflow
  , liftPhony
  , liftEff
    -- * Execution
  , FlowCtx(..)
  , runFlow
  ) where

import Control.Lens
import Control.Monad.State.Strict
import Control.Monad.Operational
import Data.Map.Strict            ((!))
import Data.Map.Strict            qualified as Map
import Data.Set                   qualified as Set

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Types
import OKA.Flow.Run

----------------------------------------------------------------
-- Flow monad
----------------------------------------------------------------

-- | We want given workflow evaluated
want :: ResultSet a => a -> Flow res eff ()
want a = Flow $ _2 . flowTgtL %= (<> Set.fromList (toResultSet a))

-- | Create new primitive flow.
--
--   This function does not provide any type safety by itself
liftWorkflow
  :: (ResultSet params)
  => Workflow res -- ^ Executioner.
  -> params       -- ^ Parameters
  -> Flow res eff (Result a)
liftWorkflow exe p = Flow $ do
  (meta,gr) <- get
  -- Allocate new
  let fid = case Map.lookupMax (flowGraph gr) of
              Just (FunID i, _) -> FunID (i + 1)
              Nothing           -> FunID 0
  -- Dependence on function without result is an error
  let res = toResultSet p
      phonyDep i = isPhony $ funWorkflow $ flowGraph gr ! i
  when (any phonyDep res) $ do
    error "Depending on phony target"
  -- Add workflow to graph
  _2 . flowGraphL . at fid .= Just Fun
    { funWorkflow = exe
    , funMetadata = meta
    , funOutput   = ()
    , funParam    = res
    }
  return $ Result fid

-- | Lift phony workflow (not checked)
liftPhony
  :: (ResultSet params)
  => (res -> Metadata -> [FilePath] -> IO ())
  -> params
  -> Flow res eff ()
liftPhony exe p = want =<< liftWorkflow (Phony exe) p

-- | Lift effect
liftEff :: eff a -> Flow res eff a
liftEff = Flow . lift . singleton
