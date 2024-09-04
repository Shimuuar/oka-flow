{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE ViewPatterns     #-}
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
  , Resource(..)
  , withResources
    -- ** Primitives for 'ResourceSet'
  , basicAddResource
  , basicRequestResource
  , basicReleaseResource
    -- * Deriving via
  , ResAsMutex(..)
  , ResAsCounter(..)
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
newtype ResourceSet = ResourceSet (Map.Map TypeRep SomeResource)
  deriving newtype (Semigroup, Monoid)

data SomeResource where
  SomeResource :: Typeable a => (a -> STM (), a -> STM ()) -> SomeResource



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


-- | Perform action which requires resource. It ensures that resource
--   is available only for it while action executes
withResources
  :: Resource res
  => ResourceSet -- ^ Set of available resources
  -> res         -- ^ Requested resources
  -> IO a        -- ^ Action to execute
  -> IO a
withResources res r = bracket_ ini fini where
  ini  = atomically $ requestResource res r
  fini = atomically $ releaseResource res r


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
  requestResource = coerce (basicRequestResource @a)
  releaseResource = coerce (basicReleaseResource @a)


----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

-- | Request nothing
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
