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
module OKA.Flow.Core.Resources
  ( -- * Resources
    ResourceSet
  , Claim(..)
  , ResourceDef(..)
  , ResourceClaim(..)
  , withResources
    -- ** Primitives for 'ResourceSet'
  , basicCreateResource
  , basicClaimResource
    -- * Deriving via
  , ResAsMutex(..)
  , ResAsCounter(..)
    -- * Standard resources
  , LockGHC(..)
  , LockCoreCPU(..)
  , LockMemGB(..)
  ) where

import Control.Applicative
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
--   available resources in types since it leads to overly complicated
--   type signatures. Instead we opt for turning unsatisfiable
--   resource requests into runtime errors.
newtype ResourceSet = ResourceSet (Map.Map TypeRep ResBox)
  deriving newtype (Semigroup, Monoid)

data ResBox where
  ResBox :: Typeable a => (a -> STM ()) -> (a -> STM ()) -> ResBox


-- | Claim on resources which are required for evaluation of single
--   flow. This is STM transaction which claims resources and returns
--   another transaction for releasing them.
newtype Claim = Claim
  { claim :: ResourceSet -> STM (STM ())
  }

instance Semigroup Claim where
  Claim l1 <> Claim l2 = Claim $ (liftA2 . liftA2) (>>) l1 l2

instance Monoid Claim where
  mempty = Claim mempty


-- | Way to construct resource set
class Typeable a => ResourceDef a where
  createResource :: a -> IO ResourceSet
  
class Typeable a => ResourceClaim a where
  claimResource :: a -> Claim



instance ResourceDef () where
  createResource = pure mempty

instance (ResourceDef a, ResourceDef b) => ResourceDef (a,b) where
  createResource (a,b) = do
    ra <- createResource a
    rb <- createResource b
    pure $ ra <> rb

instance (ResourceDef a, ResourceDef b, ResourceDef c) => ResourceDef (a,b,c) where
  createResource (a,b,c) = do
    ra <- createResource a
    rb <- createResource b
    rc <- createResource c
    pure $ ra <> rb <> rc

instance (ResourceDef a, ResourceDef b, ResourceDef c, ResourceDef d
         ) => ResourceDef (a,b,c,d) where
  createResource (a,b,c,d) = do
    ra <- createResource a
    rb <- createResource b
    rc <- createResource c
    rd <- createResource d
    pure $ ra <> rb <> rc <> rd

  


instance ResourceClaim Claim where
  claimResource = id

instance ResourceClaim () where
  claimResource = mempty

instance (ResourceClaim a, ResourceClaim b) => ResourceClaim (a,b) where
  claimResource (a,b) = 
    claimResource a <> claimResource b

instance (ResourceClaim a, ResourceClaim b, ResourceClaim c) => ResourceClaim (a,b,c) where
  claimResource (a,b,c) = 
    claimResource a <> claimResource b <> claimResource c

instance (ResourceClaim a, ResourceClaim b, ResourceClaim c, ResourceClaim d
         ) => ResourceClaim (a,b,c,d) where
  claimResource (a,b,c,d) = 
    claimResource a <> claimResource b <> claimResource c <> claimResource d







-- | Primitive for adding resource to set of resources
basicCreateResource
  :: forall a. Typeable a
  => (a -> STM (), a -> STM ()) -- ^ Pair of request\/release functions
  -> ResourceSet
basicCreateResource (l,u) = do
  ResourceSet $ Map.singleton (typeOf (undefined :: a)) (ResBox l u)

-- | Primitive for requesting resource for evaluation. Should be only
--   used for 'ResourceClaim' instances definition
basicClaimResource :: forall a. Typeable a => a -> Claim
basicClaimResource a = Claim $ \(ResourceSet m) -> do
  case typeOf a `Map.lookup` m of
    Nothing -> error $ unlines
      $ ("requestResource: " ++ show (typeOf a) ++ " is not available. Present:")
      : [ " - " ++ show k
        | k <- Map.keys m
        ]
    Just (ResBox lock unlock)
      | Just lock'   <- cast @_ @(a -> STM ()) lock
      , Just unlock' <- cast @_ @(a -> STM ()) unlock
        -> unlock' a <$ lock' a
      | otherwise -> error $ "requestResource: INTERNAL ERROR"





-- | Perform action which requires resource. It ensures that resource
--   is available only for it while action executes
withResources
  :: ResourceClaim res
  => ResourceSet -- ^ Set of available resources
  -> res         -- ^ Requested resources
  -> IO a        -- ^ Action to execute
  -> IO a
withResources res r = bracket ini fini . const where
  claim = claimResource r
  ini   = atomically $ claim.claim res
  fini  = atomically


----------------------------------------------------------------
-- Deriving via
----------------------------------------------------------------

-- | Derive resource for mutex-like lock for resource where only one
--   instance could be used at a time.
newtype ResAsMutex a = ResAsMutex a

instance Typeable a => ResourceDef (ResAsMutex a) where
  createResource _ = do
    lock <- newTMVarIO ()
    pure $! basicCreateResource @a
      ( \_ -> takeTMVar lock
      , \_ -> putTMVar  lock ()
      )

instance Typeable a => ResourceClaim (ResAsMutex a) where
  claimResource (ResAsMutex a) = basicClaimResource a

-- | Derive resource where there're N instance of resource and we
--   can't use more than that. Flow may request more than one.
newtype ResAsCounter a = ResAsCounter a

instance (Typeable a, Coercible a Int) => ResourceDef (ResAsCounter a) where
  createResource (ResAsCounter (coerce -> n)) = do
    counter <- newTVarIO (n :: Int)
    pure $! basicCreateResource @a
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


instance (Typeable a) => ResourceClaim (ResAsCounter a) where
  claimResource (ResAsCounter a) = basicClaimResource a




----------------------------------------------------------------
-- Standard resources
----------------------------------------------------------------


-- | We want to have one concurrent build. This data type provides mutex
data LockGHC = LockGHC
  deriving stock (Show,Eq)
  deriving (ResourceDef,ResourceClaim) via ResAsMutex LockGHC

-- | Number of CPU cores that flow is allowed to utilize.
newtype LockCoreCPU = LockCoreCPU Int
  deriving stock (Show,Eq)
  deriving (ResourceDef,ResourceClaim) via ResAsCounter LockCoreCPU

-- | How much memory flow is expected to use in GB
newtype LockMemGB = LockMemGB Int
  deriving stock (Show,Eq)
  deriving (ResourceDef,ResourceClaim) via ResAsCounter LockMemGB
