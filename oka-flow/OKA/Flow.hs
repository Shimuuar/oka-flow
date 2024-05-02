{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost        #-}
{-# LANGUAGE OverloadedRecordDot        #-}
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
  , scopeMeta
  , restrictMeta
  , Result
  , ResultSet(..)
  , want
  , liftWorkflow
  , liftPhony
  , liftEff
    -- * Resources
  , Resource(..)
  , ResAsMutex(..)
  , ResAsCounter(..)
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
want :: ResultSet a => a -> Flow eff ()
want a = Flow $ _2 . flowTgtL %= (<> Set.fromList (toResultSet a))

basicLiftWorkflow
  :: (ResultSet params, Resource res)
  => res
  -> Workflow -- ^ Executioner.
  -> params   -- ^ Parameters
  -> Flow eff (Result a)
basicLiftWorkflow resource exe p = Flow $ do
  (meta,gr) <- get
  -- Allocate new
  let fid = case Map.lookupMax gr.graph of
              Just (FunID i, _) -> FunID (i + 1)
              Nothing           -> FunID 0
  -- Dependence on function without result is an error
  let res = toResultSet p
      phonyDep i = isPhony $ (gr.graph ! i).workflow
  when (any phonyDep res) $ do
    error "Depending on phony target"
  -- Add workflow to graph
  _2 . flowGraphL . at fid .= Just Fun
    { workflow   = exe
    , metadata   = meta
    , output     = ()
    , requestRes = \r -> requestResource r resource
    , releaseRes = \r -> releaseResource r resource
    , param      = res
    }
  return $ Result fid

-- | Create new primitive workflow.
--
--   This function does not provide any type safety by itself!
liftWorkflow
  :: (ResultSet params, Resource res)
  => res    -- ^ Resources required by workflow
  -> Action -- ^ Action to execute
  -> params -- ^ Parameters
  -> Flow eff (Result a)
liftWorkflow res action p = basicLiftWorkflow res (Workflow action) p


-- | Lift phony workflow (not checked)
liftPhony
  :: (ResultSet params, Resource res)
  => res
     -- ^ Resources required by
  -> (ResourceSet -> Metadata -> [FilePath] -> IO ())
     -- ^ Action to execute
  -> params
     -- ^ Parameters to pass to workflow
  -> Flow eff ()
liftPhony res exe p = want =<< basicLiftWorkflow res (Phony exe) p

-- | Lift effect
liftEff :: eff a -> Flow eff a
liftEff = Flow . lift . singleton
