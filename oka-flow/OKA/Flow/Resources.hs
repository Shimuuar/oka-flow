-- |
-- Very simple resource management system. We need way to observe
-- resource limitation when scheduling workflows. We use very simple
-- system where each workflow has estimate of resource required for
-- execution and scheduler ensures that we never try to run more
-- workflows that there are available resources.
--
-- We don't try to track which resource workflow uses in types since
-- it will lead to cumbersome type signatures. Instead we opaque
-- collection of resources. It works fine in practice.
--
-- Note that we treat tuples as collections. @()@ is used to denote
-- request of no resource and for example @(A,B)@ requests both @A@
-- and @B@.
module OKA.Flow.Resources
  ( -- * Resources
    ResourceSet
  , Lock(..)
  , Resource(..)
  , withResources
    -- ** Primitives for 'ResourceSet'
  , basicAddResource
  , basicRequestResource
  , basicReleaseResource
    -- * Deriving via
  , ResAsMutex(..)
  , ResAsCounter(..)
    -- * Standard resources
  , SomeResource(..)
  , LockGHC(..)
  , LockCoreCPU(..)
  , LockMemGB(..)
  ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Coerce
import Data.Typeable
import Data.Map.Strict    qualified as Map


----------------------------------------------------------------
-- Resource set
----------------------------------------------------------------

-- | Set of resources available to evaluator. We don't try to track
--   available resources in types since it leads to complicated type
--   signatures. Instead we opt for turning unsatisfiable resource
--   requests into runtime errors.
newtype ResourceSet = ResourceSet (Map.Map TypeRep ResBox)
  deriving newtype (Semigroup, Monoid)

data ResBox where
  ResBox :: Typeable a => (a -> STM (), a -> STM ()) -> ResBox


-- | Data structure which provides pair of STM transaction which are
--   used for acquiring and releasing resources.
data Lock = Lock
  { acquire :: ResourceSet -> STM ()
  , release :: ResourceSet -> STM ()
  }

instance Semigroup Lock where
  Lock l1 u1 <> Lock l2 u2 = Lock (l1 <> l2) (u1 <> u2)
instance Monoid Lock where
  mempty = Lock mempty mempty



-- | Primitive for adding resource to set of resources
basicAddResource
  :: forall a. Typeable a
  => ResourceSet                -- ^ set of resources
  -> (a -> STM (), a -> STM ()) -- ^ Pair of request\/release functions
  -> ResourceSet
basicAddResource (ResourceSet m) fun = do
  ResourceSet $ Map.insert (typeOf (undefined :: a)) (ResBox fun) m

-- | Primitive for requesting resource for evaluation. Should be only
--   used for 'Resource' instances definition
basicRequestResource :: forall a. Typeable a => a -> ResourceSet -> STM ()
basicRequestResource a (ResourceSet m) =
  case typeOf a `Map.lookup` m of
    Nothing -> error $ unlines
      $ ("requestResource: " ++ show (typeOf a) ++ " is not available. Present:")
      : [ " - " ++ show k
        | k <- Map.keys m
        ]
    Just (ResBox (lock,_))
      | Just lock' <- cast lock -> lock' a
      | otherwise               -> error $ "requestResource: INTERNAL ERROR"

-- | Primitive for releasing resource after evaluation. Should be only
--   used for 'Resource' instances definition
basicReleaseResource :: forall a. Typeable a => a -> ResourceSet -> STM ()
basicReleaseResource a (ResourceSet m) =
  case typeOf a `Map.lookup` m of
    Nothing -> error $ "releaseResource: " ++ show (typeOf a) ++ " is not available"
    Just (ResBox (_,unlock))
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
  resourceLock :: a -> Lock


-- | Perform action which requires resource. It ensures that resource
--   is available only for it while action executes
withResources
  :: Resource res
  => ResourceSet -- ^ Set of available resources
  -> res         -- ^ Requested resources
  -> IO a        -- ^ Action to execute
  -> IO a
withResources res r = bracket_ ini fini where
  lock = resourceLock r
  ini  = atomically $ lock.acquire res
  fini = atomically $ lock.release res


----------------------------------------------------------------
-- Deriving via
----------------------------------------------------------------

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
  resourceLock a = Lock { acquire = basicRequestResource a
                        , release = basicReleaseResource a
                        }

-- | Derive resource where there're N instance of resource and we
--   can't use more than that. Flow may request more than one.
newtype ResAsCounter a = ResAsCounter a

instance (Typeable a, Coercible a Int) => Resource (ResAsCounter a) where
  createResource (ResAsCounter (coerce -> n)) r = do
    counter <- newTVarIO (n :: Int)
    pure $! basicAddResource @a r
      ( \(coerce -> k) -> do
            when (k < 0) $ error $
              "Resource[" <> show ty <> "]: negative amount requested"
            when (k > n) $ error $
              "Resource[" <> show ty <> "]: request cannot be satisfied"
            modifyTVar' counter (subtract k)
            check . (>= 0) =<< readTVar counter
      , \(coerce -> k) -> do
            when (k < 0) $ error $
              "Resource[" <> show ty <> "]: negative amount requested"
            modifyTVar' counter (+ k)
      )
    where ty = typeOf (undefined :: a)
  resourceLock a = Lock { acquire = basicRequestResource a
                        , release = basicReleaseResource a
                        }


----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

-- | Request nothing
instance Resource () where
  createResource = pure pure
  resourceLock   = mempty


instance (Resource a, Resource b) => Resource (a,b) where
  createResource (a,b) =  createResource b
                      <=< createResource a
  resourceLock (a,b) = resourceLock a <> resourceLock b

instance (Resource a, Resource b, Resource c) => Resource (a,b,c) where
  createResource (a,b,c)
    =  createResource c
   <=< createResource b
   <=< createResource a
  resourceLock (a,b,c) = resourceLock a <> resourceLock b <> resourceLock c

instance (Resource a, Resource b, Resource c, Resource d) => Resource (a,b,c,d) where
  createResource (a,b,c,d)
    =  createResource d
   <=< createResource c
   <=< createResource b
   <=< createResource a
  resourceLock (a,b,c,d) = resourceLock a <> resourceLock b <> resourceLock c <> resourceLock d



----------------------------------------------------------------
-- Standard resources
----------------------------------------------------------------

-- | Existential wrapper for resource
data SomeResource where
  SomeResource :: Resource a => a -> SomeResource

instance Resource SomeResource where
  createResource (SomeResource a) = createResource a
  resourceLock   (SomeResource a) = resourceLock   a

-- | We want to have one concurrent build. This data type provides mutex
data LockGHC = LockGHC
  deriving stock (Show,Eq)
  deriving Resource via ResAsMutex LockGHC

-- | Number of CPU cores that flow is allowed to utilize.
newtype LockCoreCPU = LockCoreCPU Int
  deriving stock (Show,Eq)
  deriving Resource via ResAsCounter LockCoreCPU

-- | How much memory flow is expected to use in GB
newtype LockMemGB = LockMemGB Int
  deriving stock (Show,Eq)
  deriving Resource via ResAsCounter LockMemGB
