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
    -- * Flow monad
  , Flow(..)
  , Result
  , ResultSet(..)
  , want
  , liftWorkflow
  ) where

import Control.Lens
import Control.Monad.Reader
import Control.Monad.State.Strict
import Control.Monad.Operational
import Data.Map.Strict    (Map,(!))
import Data.Map.Strict    qualified as Map
import Data.Set    (Set)
import Data.Set    qualified as Set

import OKA.Metadata
import OKA.Flow.Graph


----------------------------------------------------------------
-- Flow monad
----------------------------------------------------------------

-- | Flow monad which we use to build workflow
newtype Flow res eff a = Flow
  (ReaderT Metadata (StateT (FlowGraph res ()) (Program eff)) a)
  deriving newtype (Functor, Applicative, Monad)

-- | We want given workflow evaluated
want :: Result a -> Flow res eff ()
want (Result i) = Flow $ flowTgtL %= Set.insert i

-- | Create new primitive flow.
--
--   This function does not provide any type safety by itself 
liftWorkflow
  :: (ResultSet params)
  => Workflow res -- ^ Executioner.
  -> params       -- ^ Parameters 
  -> Flow res eff (Result a)
liftWorkflow exe p = Flow $ do
  meta <- ask
  gr   <- get
  -- Allocate new 
  let fid = case Map.lookupMax (flowGraph gr) of
              Just (FunID i, _) -> FunID (i + 1)
              Nothing           -> FunID 0
  -- Dependence on function without result is an error
  let res = toResultSet p
      phonyDep fid = isPhony $ funWorkflow $ flowGraph gr ! fid
  when (any phonyDep res) $ do
    error "Depending on phony target"
  -- Add workflow to graph
  put $! gr & flowGraphL . at fid .~ Just Fun
    { funWorkflow = exe
    , funMetadata = meta
    , funOutput   = (fid,())
    , funParam    = (,()) <$> res
    }
  return $ Result fid


----------------------------------------------------------------
-- Sugar
----------------------------------------------------------------

-- | Opaque handle to result of evaluation of single dataflow
--   function. It doesn't contain any real data and in fact is just a
--   promise to evaluate result.
newtype Result a = Result FunID

-- | Data types which could be used as parameters to dataflow functions
class ResultSet a where
  toResultSet :: a -> [FunID]

instance ResultSet () where
  toResultSet () = []

instance ResultSet (Result a) where
  toResultSet (Result i) = [i]

instance (ResultSet a, ResultSet b) => ResultSet (a,b) where
  toResultSet (a,b) = toResultSet a <> toResultSet b

instance (ResultSet a, ResultSet b, ResultSet c) => ResultSet (a,b,c) where
  toResultSet (a,b,c) = toResultSet a <> toResultSet b <> toResultSet c
