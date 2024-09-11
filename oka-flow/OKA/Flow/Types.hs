{-# LANGUAGE UndecidableInstances  #-}
-- |
-- Basic data types used in definition of dataflow program
module OKA.Flow.Types
  ( -- * Workflow primitives
    Action(..)
  , Executable(..)
  , Workflow(..)
  , isPhony
    -- * Store objects API
  , FunID(..)
  , Result(..)
  , ResultSet(..)
    -- ** Store path
  , Hash(..)
  , StorePath(..)
  , storePath
  ) where

import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import Data.Coerce
import System.FilePath        ((</>))
import GHC.Generics

import OKA.Metadata           (Metadata)
import OKA.Flow.Resources


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

-- | Action which execute executable. It should be always used if one
--   want to call external executable. This way executor can correctly
--   pass metadata to it.
data Executable = Executable
  { name :: String
    -- ^ Name of a workflow
  , executable :: FilePath
    -- ^ Executable to start
  , io         :: ResourceSet -> IO ()
    -- ^ IO action which could be executed to prepare program.
  }



-- | Descritpion of workflow function. It knows how to build
data Workflow
  = Workflow Action
    -- ^ Standard workflow which executes haskell action
  | WorkflowExe Executable
    -- ^ Target which run executable
  | Phony    (ResourceSet -> Metadata -> [FilePath] -> IO ())
    -- ^ Phony target which always executes action


isPhony :: Workflow -> Bool
isPhony = \case
  Workflow{}    -> False
  WorkflowExe{} -> False
  Phony{}       -> True



----------------------------------------------------------------
-- Store objects and interaction
----------------------------------------------------------------

-- | This type class observe isomorphism between tuples and records
--   containing 'StoreObject' and lists of underlying data types
class ResultSet a where
  toResultSet    :: a -> [FunID]


instance ResultSet () where
  toResultSet () = []

instance ResultSet (Result a) where
  toResultSet (Result i) = [i]

instance ResultSet a => ResultSet [a] where
  toResultSet    = concatMap toResultSet

deriving via Generically (a,b)
    instance (ResultSet a, ResultSet b) => ResultSet (a,b)
deriving via Generically (a,b,c)
    instance (ResultSet a, ResultSet b, ResultSet c) => ResultSet (a,b,c)
deriving via Generically (a,b,c,d)
    instance (ResultSet a, ResultSet b, ResultSet c, ResultSet d) => ResultSet (a,b,c,d)
deriving via Generically (a,b,c,d,e)
    instance (ResultSet a, ResultSet b, ResultSet c, ResultSet d, ResultSet e) => ResultSet (a,b,c,d,e)
deriving via Generically (a,b,c,d,e,f)
    instance (ResultSet a, ResultSet b, ResultSet c, ResultSet d, ResultSet e, ResultSet f) => ResultSet (a,b,c,d,e,f)



-- | Used for deriving using generics
instance (Generic a, GResultSet (Rep a)) => ResultSet (Generically a) where
  toResultSet (Generically a) = gtoResultSet (from a)

-- Type class for generics
class GResultSet f where
  gtoResultSet :: f () -> [FunID]

deriving newtype instance (GResultSet f) => GResultSet (M1 i c f)

instance (GResultSet f, GResultSet g) => GResultSet (f :*: g) where
  gtoResultSet (f :*: g) = gtoResultSet f <> gtoResultSet g

instance (ResultSet a) => GResultSet (K1 i a) where
  gtoResultSet = coerce (toResultSet @a)



----------------------------------------------------------------
-- Dataflow graph
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
