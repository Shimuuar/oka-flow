{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeFamilies        #-}
-- |
-- Standard effects for Flow monad
module OKA.Flow.Eff
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
    -- * Configuration
  , ProgConfigE
  , ProgConfig(..)
  , runProgConfigE
  , askProgConfig
    -- * PRNG
  , PrngE
  , PRNG(..)
  , runPrngE
  ) where

import Control.Exception
import Control.Monad
import Data.Coerce
import Data.Yaml                    qualified as YAML
import Data.Dynamic                 qualified as Dyn
import Data.Typeable
import Data.IORef
import Data.Map.Strict              qualified as Map
import Data.Map.Strict              (Map)
import Effectful
import Effectful.Dispatch.Dynamic
import Effectful.Dispatch.Static
import System.FilePath              ((</>))
import System.Random.Stateful
import GHC.Generics                 (Generic)

import OKA.Flow.Graph
import OKA.Metadata


----------------------------------------------------------------
-- Read metadata
----------------------------------------------------------------

-- | Effect for reading metadata from config files
data ReadMeta :: Effect where
  ReadMeta :: IsFromMeta a => FilePath -> ReadMeta m a

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


doReadMeta :: (IsFromMeta a) => FilePath -> IO a
doReadMeta path = YAML.decodeFileEither path >>= \case
  Left  e  -> error $ show e
  Right js -> case decodeMetadataEither js of
    Left  e -> throwIO e
    Right a -> pure a



-- | Read and decode YAML file using 'IsMeta' instance
loadMeta
  :: (HasCallStack, IsFromMeta a, ReadMeta :> es)
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
  loadMeta @(Optional a) path >>= \case
    Optional (Just a)  -> appendMeta a
    Optional (Nothing) -> pure ()



----------------------------------------------------------------
-- Program config
----------------------------------------------------------------

-- | Workflow has access to configuration which allows to access to
--   configuration of external tool. This is intended to allow to
--   specify various programs which should not affect output and
--   mostly used in phony workflows in order to inspect results etc.
data ProgConfigE :: Effect

type    instance DispatchOf ProgConfigE = Static NoSideEffects
newtype instance StaticRep  ProgConfigE = ProgConfigE ProgConfig

-- | Configuration of a workflow
data ProgConfig = ProgConfig
  { pdf :: Maybe FilePath
    -- ^ PDF reader to use. Uses xdg-open if not specified
  , browser :: Maybe FilePath
    -- ^ Browser to use
  }
  deriving stock (Show,Eq,Generic)
  deriving anyclass (YAML.FromJSON, YAML.ToJSON)

runProgConfigE :: ProgConfig -> Eff (ProgConfigE : es) a -> Eff es a
runProgConfigE cfg = evalStaticRep (ProgConfigE cfg)

askProgConfig :: ProgConfigE :> es => Flow es ProgConfig
askProgConfig = Flow $ do
  ProgConfigE cfg <- getStaticRep
  pure cfg


----------------------------------------------------------------
-- PRNG
----------------------------------------------------------------

-- | Ability to use standard splitmix PRNG.
data PrngE :: Effect

type    instance DispatchOf PrngE = Static NoSideEffects
newtype instance StaticRep  PrngE = PrngE StdGen

-- | Handle for using 'StatefulGen' API
data PRNG = PRNG

-- | Handle for PRNG effect
runPrngE
  :: StdGen -- ^ Initial state for a PRNG
  -> Eff (PrngE : es) a
  -> Eff es a
runPrngE g = evalStaticRep (PrngE g)

instance (PrngE :> eff) => StatefulGen PRNG (Flow eff) where
  uniformWord32    _ = Flow $ stateStaticRep @PrngE $ coerce (genWord32  @StdGen)
  uniformWord64    _ = Flow $ stateStaticRep @PrngE $ coerce (genWord64  @StdGen)
  uniformWord32R n _ = Flow $ stateStaticRep @PrngE $ coerce (genWord32R @StdGen n)
  uniformWord64R n _ = Flow $ stateStaticRep @PrngE $ coerce (genWord64R @StdGen n)
  uniformShortByteString n _ = Flow $ stateStaticRep @PrngE $ coerce (genShortByteString @StdGen n)
