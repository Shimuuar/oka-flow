{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE ImportQualifiedPost #-}
-- |
-- Basic data types used in definition of dataflow program
module OKA.Flow.Types
  ( -- * Workflow primitives
    Action(..)
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

import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import OKA.Metadata           (Metadata)


----------------------------------------------------------------
-- Workflow primitives
----------------------------------------------------------------

-- | Single action to be performed.
data Action res
  = ActNormal (res -> Metadata -> [FilePath] -> FilePath -> IO ())
    -- ^ Action which produces output
  | ActPhony  (res -> Metadata -> [FilePath] -> IO ())
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
  deriving stock (Show,Eq)

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

instance Show Hash where
  show (Hash hash) = show $ BC8.unpack $ Base16.encode hash

-- | Path in nix-like storage. 
data StorePath = StorePath String Hash
  deriving (Show)

-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm ++ "-" ++ BC8.unpack (Base16.encode hash)
