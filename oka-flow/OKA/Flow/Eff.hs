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
  ) where

import Control.Exception
import Control.Monad
import Data.Yaml                  qualified as YAML
import Effectful
import Effectful.Dispatch.Static
import System.FilePath            ((</>))

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
