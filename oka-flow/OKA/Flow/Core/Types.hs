-- |
module OKA.Flow.Core.Types
  ( FunID(..)
  , Result(..)
  , Hash(..)
  , StorePath(..)
  , storePath
  ) where

import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import System.FilePath        ((</>))

----------------------------------------------------------------
-- Nodes in dataflow graph
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

-- | Opaque handle to result of evaluation of single dataflow
--   function. It doesn't contain any real data and in fact is just a
--   promise to evaluate result.
newtype Result a = Result FunID



----------------------------------------------------------------
-- Paths in store
----------------------------------------------------------------

-- | SHA1 hash
newtype Hash = Hash ByteString
  deriving newtype (Eq,Ord)

instance Show Hash where
  show (Hash hash) = show $ BC8.unpack $ Base16.encode hash

-- | Path in nix-like storage.
data StorePath = StorePath
  { name :: String
  , hash :: Hash
  }
  deriving (Show)

-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm </> BC8.unpack (Base16.encode hash)
