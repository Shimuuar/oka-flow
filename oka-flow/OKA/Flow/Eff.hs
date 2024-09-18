{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeFamilies        #-}
-- |
-- Standard effects for Flow monad
module OKA.Flow.Eff
  ( -- * Read metadata
    ReadMeta
  , runReadMeta
  , loadMeta
  , readMeta
    -- * Configuration
  , ProgConfigE
  , ProgConfig(..)
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
import Effectful
import Effectful.Dispatch.Static
import System.FilePath              ((</>))
import System.Random.Stateful

import OKA.Flow.Graph
import OKA.Metadata


----------------------------------------------------------------
-- Read metadata
----------------------------------------------------------------

-- | Effect for reading metadata from config files
data ReadMeta :: Effect

type    instance DispatchOf ReadMeta = Static WithSideEffects
newtype instance StaticRep  ReadMeta = ReadMeta FilePath


-- | Run a 'Reader' effect with the given initial environment.
runReadMeta
  :: IOE :> es
  => FilePath              -- ^ File path directory with configs
  -> Eff (ReadMeta : es) a
  -> Eff es a
runReadMeta r = evalStaticRep (ReadMeta r)

-- | Read and decode YAML file using 'IsMeta' instance
loadMeta
  :: (IsMeta a, ReadMeta :> es)
  => FilePath -- ^ File path relative to config root
  -> Flow es a
loadMeta path = Flow $ do
  ReadMeta root <- getStaticRep
  unsafeEff_ $ YAML.decodeFileEither (root </> path) >>= \case
    Left  e  -> error $ show e
    Right js -> case decodeMetadataEither js of
      Left  e -> throwIO e
      Right a -> pure a


-- | Read, decode YAML file using 'IsMeta' instance and store it in
--   current metadata.
readMeta
  :: forall a es. (IsMeta a, ReadMeta :> es)
  => FilePath -- ^ File path relative to config root
  -> Flow es ()
readMeta = appendMeta <=< loadMeta @a



----------------------------------------------------------------
-- Program config
----------------------------------------------------------------

-- | Workflow has access to configuration which allows to access to
--   configuration of external tool. This is intended to allow to
--   specify various programs which should not affect output and
--   mostly used in phony workflows in order to inspect results etc.
data ProgConfigE :: Effect

type    instance DispatchOf ProgConfigE = Static WithSideEffects
newtype instance StaticRep  ProgConfigE = ProgConfigE ProgConfig

-- | Configuration of a workflow
data ProgConfig = ProgConfig
  { pdf :: FilePath -- ^ PDF reader to use
  }

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
