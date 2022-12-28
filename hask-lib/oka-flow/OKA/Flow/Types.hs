{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE ImportQualifiedPost #-}
-- |
-- Basic data types used in definition of dataflow program
module OKA.Flow.Types
  ( -- * Initialization & finalization
    BracketSTM(..)
  , runBracketSTM
  , checkAtStart
  , checkAtFinish
    -- * Workflow primitives
  , Action(..)
  , isPhony
  , Workflow(..)
  , FunID(..)
  , Result(..)
  , ResultSet(..)
    -- * Store
  , Hash(..)
  , StorePath(..)
  , storePath
  ) where

import Control.Monad
import Control.Monad.STM
import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import OKA.Metadata           (Metadata)

----------------------------------------------------------------
-- Initialization/finalization
----------------------------------------------------------------

-- | Bracket function which allows to acquire some resource in
--   concurrent setting and return some value and STM action which
--   releases taken resource.
newtype BracketSTM res a = BracketSTM (res -> STM (STM (), a))
  deriving (Functor)

instance Applicative (BracketSTM res) where
  pure a = BracketSTM $ \_ -> pure (pure (), a)
  (<*>)  = ap

instance Monad (BracketSTM res) where
  BracketSTM br1 >>= f = BracketSTM $ \res -> do
    (release1, a) <- br1 res
    let BracketSTM br2 = f a
    (release2, b) <- br2 res
    pure (release1 *> release2, b)


runBracketSTM :: res -> BracketSTM res a -> STM (STM (), a)
runBracketSTM res (BracketSTM f) = f res

-- | Check that only need to be completed at start of evaluation.
checkAtStart :: STM () -> BracketSTM res ()
checkAtStart chk = BracketSTM $ \_ -> (pure (), ()) <$ chk

-- | Check that only need to be completed at start of evaluation.
checkAtFinish :: STM () -> BracketSTM res ()
checkAtFinish fini = BracketSTM $ \_ -> pure (fini, ())



----------------------------------------------------------------
-- Workflow primitives
----------------------------------------------------------------

-- | Single action to be performed.
data Action res
  = ActNormal (BracketSTM res (Metadata -> [FilePath] -> FilePath -> IO ()))
    -- ^ Action which produces output
  | ActPhony  (BracketSTM res (Metadata -> [FilePath] -> IO ()))
    -- ^ Action which doesn't produce any outputs

-- | Descritpion of workflow function. It knows how to build
data Workflow res = Workflow
  { workflowRun    :: Action res
    -- ^ Start workflow. This function takes resources as input and
    --   return STM action which either returns actual IO function to
    --   run or retries if it's unable to secure necessary resources.
  , workflowName   :: String
    -- ^ Name of workflow. Used for caching
  }

isPhony :: Workflow res -> Bool
isPhony f = case workflowRun f of
  ActNormal{} -> False
  ActPhony{}  -> True


-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

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

----------------------------------------------------------------
-- Paths in store
----------------------------------------------------------------

-- | SHA1 hash
newtype Hash = Hash ByteString

-- | Path in nix-like storage. 
data StorePath = StorePath String Hash

-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm ++ "-" ++ BC8.unpack (Base16.encode hash)
