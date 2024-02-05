{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}
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
    -- * Resources
  , ResourceSet
  , Resource(..)
  , withResources
    -- ** Deriving
  , ResAsMutex(..)
  , ResAsCounter(..)
    -- ** Primitives
  , basicAddResource
  , basicRequestResource
  , basicReleaseResource
    -- * Store
  , Hash(..)
  , StorePath(..)
  , storePath
  ) where

import Control.Concurrent.STM
import Control.Exception      (bracket_)
import Control.Monad
import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import Data.Map.Strict        qualified as Map
import Data.Coerce
import Data.Typeable
import GHC.Generics
import OKA.Metadata           (Metadata)

----------------------------------------------------------------
-- Workflow primitives
----------------------------------------------------------------

-- | Single action to be performed. It contains both workflow name and
--   action to pefgorm workflow
data Action = Action
  { name :: String
  , run  :: ResourceSet -> Metadata -> [FilePath] -> FilePath -> IO ()
  }

-- | Descritpion of workflow function. It knows how to build
data Workflow
  = Workflow Action
  | Phony    (ResourceSet -> Metadata -> [FilePath] -> IO ())

isPhony :: Workflow -> Bool
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
-- Resources
----------------------------------------------------------------

-- | Set of resource as used by evaluator. We don't try to ensure that
--   all resources are available statically since it will lead to more
--   complicated type signatures and missing resource will be found
--   quite early so it makes sense to just allow it to fail at runtime.
newtype ResourceSet = ResourceSet (Map.Map TypeRep SomeResource)
  deriving newtype (Semigroup, Monoid)

data SomeResource where
  SomeResource :: Typeable a => (a -> STM (), a -> STM ()) -> SomeResource

withResources :: Resource res => ResourceSet -> res -> IO a -> IO a
withResources res r = bracket_ ini fini where
  ini  = atomically $ requestResource res r
  fini = atomically $ releaseResource res r


-- | Primitive for adding resource to set of resources
basicAddResource
  :: forall a. Typeable a
  => ResourceSet                -- ^ set of resources
  -> (a -> STM (), a -> STM ()) -- ^ Pair of request\/release functions
  -> ResourceSet
basicAddResource (ResourceSet m) fun = do
  ResourceSet $ Map.insert (typeOf (undefined :: a)) (SomeResource fun) m

-- | Primitive for requesting resource for evaluation. Should be only
--   used for 'Resource' instances definition
basicRequestResource :: forall a. Typeable a => ResourceSet -> a -> STM ()
basicRequestResource (ResourceSet m) a =
  case typeOf a `Map.lookup` m of
    Nothing -> error $ "requestResource: " ++ show (typeOf a) ++ " is not available"
    Just (SomeResource (lock,_))
      | Just lock' <- cast lock -> lock' a
      | otherwise               -> error $ "requestResource: INTERNAL ERROR"

-- | Primitive for releasing resource after evaluation. Should be only
--   used for 'Resource' instances definition
basicReleaseResource :: forall a. Typeable a => ResourceSet -> a -> STM ()
basicReleaseResource (ResourceSet m) a =
  case typeOf a `Map.lookup` m of
    Nothing -> error $ "releaseResource: " ++ show (typeOf a) ++ " is not available"
    Just (SomeResource (_,unlock))
      | Just unlock' <- cast unlock -> unlock' a
      | otherwise                   -> error $ "requestResource: INTERNAL ERROR"


-- | Type class for requesting of resources for execution of flow. It
--   could be CPU cores, memory, connections, disk IO bandwidth, etc.
--   This is to ensure that we don't go over limitation.
class Typeable a => Resource a where
  -- | Create pair of functions to request\/release resources and add
  --   them to set of resources
  createResource  :: a -> ResourceSet -> IO ResourceSet
  -- | Request resource for set and block if there isn't enough
  requestResource :: ResourceSet -> a -> STM ()
  -- | Release resource to set. Should not block
  releaseResource :: ResourceSet -> a -> STM ()


instance Resource () where
  createResource      = pure pure
  requestResource _ _ = pure ()
  releaseResource _ _ = pure ()

instance (Resource a, Resource b) => Resource (a,b) where
  createResource (a,b) =  createResource b
                      <=< createResource a
  requestResource r (a,b) = do
    requestResource r a
    requestResource r b
  releaseResource r (a,b) = do
    releaseResource r a
    releaseResource r b

instance (Resource a, Resource b, Resource c) => Resource (a,b,c) where
  createResource (a,b,c)
    =  createResource c
   <=< createResource b
   <=< createResource a
  requestResource r (a,b,c) = do
    requestResource r a
    requestResource r b
    requestResource r c
  releaseResource r (a,b,c) = do
    releaseResource r a
    releaseResource r b
    releaseResource r c

instance (Resource a, Resource b, Resource c, Resource d) => Resource (a,b,c,d) where
  createResource (a,b,c,d)
    =  createResource d
   <=< createResource c
   <=< createResource b
   <=< createResource a
  requestResource r (a,b,c,d) = do
    requestResource r a
    requestResource r b
    requestResource r c
    requestResource r d
  releaseResource r (a,b,c,d) = do
    releaseResource r a
    releaseResource r b
    releaseResource r c
    releaseResource r d



-- | Derive resource for mutex-like lock for resource where only one
--   instance could be used at a time.
newtype ResAsMutex a = ResAsMutex a

instance Typeable a => Resource (ResAsMutex a) where
  createResource _ r = do
    lock <- newTMVarIO ()
    pure $! basicAddResource @a r
      ( \_ -> takeTMVar lock
      , \_ -> putTMVar  lock ()
      )
  requestResource = coerce (basicRequestResource @a)
  releaseResource = coerce (basicReleaseResource @a)


-- | Derive resource where there're N instance of resource and we
--   can't use more than that. Flow may request more than one.
newtype ResAsCounter a = ResAsCounter a

instance (Typeable a, Coercible a Int) => Resource (ResAsCounter a) where
  createResource (ResAsCounter (coerce -> n)) r = do
    counter <- newTVarIO (n :: Int)
    pure $! basicAddResource @a r
      ( \(coerce -> k) -> do
            when (k < 0) $ error $ show $
              "Resource[" <> show ty <> "]: negative amount requested"
            when (k > n) $ error $ show $
              "Resource[" <> show ty <> "]: request cannot be satisfied"
            modifyTVar' counter (subtract k)
            check . (>= 0) =<< readTVar counter
      , \(coerce -> k) -> do
            when (k < 0) $ error $ show $
              "Resource[" <> show ty <> "]: negative amount requested"
            modifyTVar' counter (+ k)
      )
    where ty = typeOf (undefined :: a)
  requestResource = coerce (basicRequestResource @a)
  releaseResource = coerce (basicReleaseResource @a)


----------------------------------------------------------------
-- Paths in store
----------------------------------------------------------------

-- | SHA1 hash
newtype Hash = Hash ByteString
  deriving newtype (Eq,Ord)

instance Show Hash where
  show (Hash hash) = show $ BC8.unpack $ Base16.encode hash

-- | Path in nix-like storage.
data StorePath = StorePath String Hash
  deriving (Show)

-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm ++ "-" ++ BC8.unpack (Base16.encode hash)
