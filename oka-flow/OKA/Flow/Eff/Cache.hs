{-# LANGUAGE DataKinds    #-}
{-# LANGUAGE TypeFamilies #-}
-- |
-- This module provides caching effect. Sometimes it's desirable to
-- cache computations which are run every time. Semantics of this
-- cache is specifically tailored for 'Flow' monad.
module OKA.Flow.Eff.Cache
  ( CacheE
  , runCacheE
  , cache
  , memoize
  ) where

import Control.Monad.State.Strict
import Data.Typeable
import Data.Map.Strict              qualified as Map
import Data.IORef
import Effectful
import Effectful.Dispatch.Dynamic
import Effectful.State.Static.Local qualified as Eff

import OKA.Flow
import OKA.Flow.Core.Graph

-- | Effect which provides ability to create caches.
data CacheE :: Effect where
  -- | Create new cache
  NewMutCache :: CacheE m (MutCache a b)
  -- | Execute cached action
  Cached :: Ord a
         => (a -> m b)
         -> MutCache a b
         -> a
         -> CacheE m b
  -- | Cache using type as a key
  SCache :: (Typeable key, Typeable a, Typeable b, Ord a)
         => Proxy key
         -> (a -> m b)
         -> a
         -> CacheE m b

type instance DispatchOf CacheE = 'Effectful.Dynamic

-- Mutable cache
newtype MutCache a b = MutCache (IORef (Map.Map a b))

data DynCache where
  DynCache :: (Typeable a, Typeable b) => Map.Map a b -> DynCache


-- | Execute caching effect
runCacheE
  :: (IOE :> es)
  => Eff (CacheE : es) a
  -> Eff es a
runCacheE action = do
  glb_cache <- liftIO $ newIORef (Map.empty :: Map.Map TypeRep DynCache)
  interpret
    (\env -> \case
      NewMutCache ->
        liftIO (MutCache <$> newIORef Map.empty)
      Cached fun (MutCache cache_ref) a ->
        localSeqUnliftIO env $ \unlift -> do
          m <- readIORef cache_ref
          case a `Map.lookup` m of
            Just b  -> pure b
            Nothing -> do
              b <- unlift $ fun a
              modifyIORef' cache_ref $ Map.insert a b
              pure b
      SCache proxy fun a -> do
        let key = keyType proxy fun
        localSeqUnliftIO env $ \unlift -> do
          dyn_dct <- readIORef glb_cache
          case key `Map.lookup` dyn_dct of
            Nothing -> do
              b <- unlift $ fun a
              modifyIORef' glb_cache $ Map.insert key (DynCache $ Map.singleton a b)
              pure b
            Just (DynCache dyn)
              | Just dct <- cast dyn -> do
                  case a `Map.lookup` dct of
                    Just b  -> pure b
                    Nothing -> do
                      b <- unlift $ fun a
                      modifyIORef' glb_cache $ Map.insert key
                        (DynCache $ Map.insert a b dct)
                      pure b
              | otherwise -> error "runCacheE: internal error"
            
    ) action
  where
    keyType :: forall key a b m. (Typeable key, Typeable a, Typeable b)
            => Proxy key -> (a -> m b) -> TypeRep
    keyType _ _ = typeOf (undefined :: (key,a,b))


-- | Create memoized function. For each @a@ it will be executed only
--   once. In order to ensure consistency function will be executed in
--   context of metadata at from call site of 'cache' and all changes
--   made to metadata by called function are discarded.
cache
  :: (Ord a, CacheE :> eff)
  => (a -> Flow eff b) -- ^ Function to memoize.
  -> Flow eff (a -> Flow eff b)
cache fun = Flow $ do
  cache_ref    <- send NewMutCache
  FlowSt{meta} <- Eff.get
  pure $ Flow . send . Cached (\a -> case scopeMeta (put meta *> fun a) of
                                 Flow m -> m
                              ) cache_ref

-- | Memoize function. Each memoized function should use different type is key.
--   Types are used as entities which are cheap to create and globally unique.
--
--   Function will be called with empty metadata and all changes to
--   metadata will be discarded.
memoize
  :: (Ord a, Typeable key, Typeable a, Typeable b, CacheE :> eff)
  => Proxy key         -- ^ Memoization key
  -> (a -> Flow eff b) -- ^ Function to memoize
  -> (a -> Flow eff b)
memoize key fun a = Flow $ do
  send (SCache key (\x -> case withEmptyMeta (fun x) of Flow m -> m) a)
