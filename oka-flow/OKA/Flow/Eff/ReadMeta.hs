{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
-- |
-- Standard effects for Flow monad
module OKA.Flow.Eff.ReadMeta
  ( -- * Read metadata
    ReadMeta
    -- ** Effect handlers
  , runReadMeta
  , runCachedReadMeta
  , ReadMetaCache
  , newReadMetaCache
    -- ** Primitives
  , loadMeta
  , readMeta
  , readOptionalMeta
  ) where

import Control.Exception
import Control.Monad
import Data.Yaml                    qualified as YAML
import Data.Dynamic                 qualified as Dyn
import Data.Typeable
import Data.IORef
import Data.Map.Strict              qualified as Map
import Data.Map.Strict              (Map)
import Effectful
import Effectful.Dispatch.Dynamic
import System.FilePath              ((</>))

import OKA.Flow.Core.Flow
import OKA.Metadata


----------------------------------------------------------------
-- Read metadata
----------------------------------------------------------------

-- | Effect for reading metadata from config files
data ReadMeta :: Effect where
  ReadMeta :: IsMeta a => FilePath -> ReadMeta m a

type instance DispatchOf ReadMeta = Dynamic

-- | Run a 'Reader' effect with the given initial environment.
runReadMeta
  :: (HasCallStack, IOE :> es)
  => FilePath              -- ^ File path directory with configs
  -> Eff (ReadMeta : es) a
  -> Eff es a
runReadMeta root = interpret $ \_ (ReadMeta path) -> liftIO $ doReadMeta (root </> path)



-- | Cache and root of metadata files
newtype ReadMetaCache = ReadMetaCache (IORef (Map (FilePath,TypeRep) Dyn.Dynamic))

newReadMetaCache :: IO ReadMetaCache
newReadMetaCache = ReadMetaCache <$> newIORef mempty

-- | Create handler for 'ReadMeta' effect which caches reads from
--   config files
runCachedReadMeta
  :: (HasCallStack, IOE :> es)
  => ReadMetaCache
  -> FilePath
  -> Eff (ReadMeta : es) a
  -> Eff es a
runCachedReadMeta cache root 
  = interpret $ \_ cmd -> liftIO $ handlerCachedReadMeta cache root cmd

handlerCachedReadMeta :: forall a eff. ReadMetaCache -> FilePath -> ReadMeta eff a -> IO a
handlerCachedReadMeta (ReadMetaCache cache_ref) root (ReadMeta path) = do
  cache <- readIORef cache_ref
  let key = (path, typeOf (undefined :: a))
  case Dyn.fromDynamic =<< (key `Map.lookup` cache) of
    Just  a -> pure a
    Nothing -> do a <- doReadMeta (root </> path)
                  a <$ modifyIORef' cache_ref (Map.insert key (Dyn.toDyn a))


doReadMeta :: (IsMeta a) => FilePath -> IO a
doReadMeta path = YAML.decodeFileEither path >>= \case
  Left  e  -> error $ show e
  Right js -> case decodeMetadataEither js of
    Left  e -> throwIO e
    Right a -> pure a



-- | Read and decode YAML file using 'IsMeta' instance
loadMeta
  :: (HasCallStack, IsMeta a, ReadMeta :> es)
  => FilePath -- ^ File path relative to config root
  -> Flow es a
loadMeta = Flow . send . ReadMeta

-- | Read, decode YAML file using 'IsMeta' instance and store it in
--   current metadata.
readMeta
  :: forall a es. (HasCallStack, IsMeta a, ReadMeta :> es)
  => FilePath -- ^ File path relative to config root
  -> Flow es ()
readMeta = appendMeta <=< loadMeta @a

-- | Read, decode YAML file using 'IsMeta' instance and store it in
--   current metadata.
readOptionalMeta
  :: forall a es. (HasCallStack, IsMeta a, ReadMeta :> es)
  => FilePath -- ^ File path relative to config root
  -> Flow es ()
readOptionalMeta path = do
  loadMeta @(Maybe a) path >>= \case
    Just a  -> appendMeta a
    Nothing -> pure ()
