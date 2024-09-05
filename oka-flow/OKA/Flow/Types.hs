{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE UndecidableInstances  #-}
-- |
-- Basic data types used in definition of dataflow program
module OKA.Flow.Types
  ( -- * Workflow primitives
    Action(..)
  , isPhony
  , Workflow(..)
  , FunID(..)
  , StoreObject(..)
  , Result
  , ObjProduct(..)
  , ResultSet
    -- * Store
  , Hash(..)
  , StorePath(..)
  , storePath
  ) where

import Control.Applicative
import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import Data.Coerce
import System.FilePath        ((</>))
import GHC.Generics

import OKA.Metadata           (Metadata)
import OKA.Flow.Resources
import OKA.Flow.Parser


----------------------------------------------------------------
-- Workflow primitives
----------------------------------------------------------------

-- | Single action to be performed. It contains both workflow name and
--   action to pefgorm workflow
data Action = Action
  { name :: String
    -- ^ Name of a workflow
  , run  :: ResourceSet -> Metadata -> [FilePath] -> FilePath -> IO ()
    -- ^ Execute action on store
  }

-- | Descritpion of workflow function. It knows how to build
data Workflow
  = Workflow Action
    -- ^ Standard workflow which executes haskell action
  | Phony    (ResourceSet -> Metadata -> [FilePath] -> IO ())
    -- ^ Phony target which always executes action


isPhony :: Workflow -> Bool
isPhony = \case
  Workflow{} -> False
  Phony{}    -> True



----------------------------------------------------------------
-- Store objects
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

newtype StoreObject r a = StoreObject r
  deriving stock (Show,Eq)


class ObjProduct r a where
  toResultSet :: a -> [r]
  parseResultSet :: ListParser r a

-- | Opaque handle to result of evaluation of single dataflow
--   function. It doesn't contain any real data and in fact is just a
--   promise to evaluate result.
type Result = StoreObject FunID

-- | Data types which could be used as parameters to dataflow functions
type ResultSet = ObjProduct FunID


instance ObjProduct r () where
  toResultSet () = []
  parseResultSet = pure ()

instance ObjProduct r (StoreObject r a) where
  toResultSet (StoreObject i) = [i]
  parseResultSet = StoreObject <$> consume

instance ObjProduct r a => ObjProduct r [a] where
  toResultSet    = concatMap toResultSet
  parseResultSet = many parseResultSet

instance (ObjProduct r a, ObjProduct r b) => ObjProduct r (a,b) where
  toResultSet (a,b) = toResultSet a <> toResultSet b
  parseResultSet    = (,) <$> parseResultSet <*> parseResultSet

instance (ObjProduct r a, ObjProduct r b, ObjProduct r c) => ObjProduct r (a,b,c) where
  toResultSet (a,b,c) = toResultSet a <> toResultSet b <> toResultSet c
  parseResultSet = (,,) <$> parseResultSet <*> parseResultSet <*> parseResultSet

instance (Generic a, GObjProduct r (Rep a)) => ObjProduct r (Generically a) where
  toResultSet (Generically a) = gtoResultSet (from a)
  parseResultSet = Generically . to <$> gparseResultSet

-- Type class for generics
class GObjProduct r f where
  gtoResultSet :: f () -> [r]
  gparseResultSet :: ListParser r (f ())

deriving newtype instance (GObjProduct r f) => GObjProduct r (M1 i c f)

instance (GObjProduct r f, GObjProduct r g) => GObjProduct r (f :*: g) where
  gtoResultSet (f :*: g) = gtoResultSet f <> gtoResultSet g
  gparseResultSet = (:*:) <$> gparseResultSet <*> gparseResultSet

instance (ObjProduct r a) => GObjProduct r (K1 i a) where
  gtoResultSet    = coerce (toResultSet    @r @a)
  gparseResultSet = coerce (parseResultSet @r @a)



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
