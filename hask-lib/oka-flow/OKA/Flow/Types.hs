{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE UndecidableInstances #-}
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
import Data.Coerce
import GHC.Generics
import OKA.Metadata           (Metadata)

----------------------------------------------------------------
-- Workflow primitives
----------------------------------------------------------------

-- | Single action to be performed. It contains both workflow name and
--   action to pefgorm workflow
data Action res = Action
  { workflowName :: String
  , workflowRun  :: res -> Metadata -> [FilePath] -> FilePath -> IO ()
  }

-- | Descritpion of workflow function. It knows how to build
data Workflow res
  = Workflow (Action res)
  | Phony    (res -> Metadata -> [FilePath] -> IO ())

isPhony :: Workflow res -> Bool
isPhony = \case
  Workflow{} -> False
  Phony{}    -> True

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

instance ResultSet a => ResultSet [a] where
  toResultSet = concatMap toResultSet

instance (ResultSet a, ResultSet b) => ResultSet (a,b) where
  toResultSet (a,b) = toResultSet a <> toResultSet b

instance (ResultSet a, ResultSet b, ResultSet c) => ResultSet (a,b,c) where
  toResultSet (a,b,c) = toResultSet a <> toResultSet b <> toResultSet c


instance (Generic a, GResultSet (Rep a)) => ResultSet (Generically a) where
  toResultSet (Generically a) = gtoResultSet (from a)

class GResultSet f where
  gtoResultSet :: f () -> [FunID]

instance (GResultSet f) => GResultSet (M1 i c f) where
  gtoResultSet = coerce (gtoResultSet @f)

instance (GResultSet f, GResultSet g) => GResultSet (f :*: g) where
  gtoResultSet (f :*: g) = gtoResultSet f <> gtoResultSet g

instance (ResultSet a) => GResultSet (K1 i a) where
  gtoResultSet = coerce (toResultSet @a)



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
