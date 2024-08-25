{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE UndecidableInstances  #-}
-- |
-- Metadata for data analysis. Usually any nontrivial data analysis
-- requires specifying multiple groups of parameters and without some
-- overarching methodology it's difficult to work with. We need
-- following features:
--
-- 1. Standard and easy way for JSON\/YAML marshalling.
--
-- 2. Ability to either specify all necessary types statically or to
--    work with dynamic dictionary of parameters.
--
-- We model each group as ordinary haskell records. For purposes of
-- serialization they are leaves on JSON tree.
module OKA.Metadata
  ( -- * Metadata
    Metadata
  , metadata
  , metadataF
  , metadataMay
  , restrictMetaByType
  , restrictMetaByKeys
  , deleteFromMetaByType
  , deleteFromMetaByKeys
  , IsMeta(toMetadata, fromMetadata)
    -- ** Encoding & decoding
  , encodeMetadataDynEither
  , encodeMetadataDyn
  , encodeMetadataEither
  , encodeMetadata
  , decodeMetadataEither
  , decodeMetadata
  , readMetadataEither
  , readMetadata
    -- * JSON serialization
  , MetaEncoding(..)
    -- ** Deriving via
  , AsAeson(..)
  , AsReadShow(..)
  , AsRecord(..)
  , AsMeta(..)
  ) where

import Control.Exception          (throw,throwIO)
import Control.Monad.IO.Class
import Control.Lens

import Data.Yaml                  qualified as YAML

import OKA.Metadata.Encoding
import OKA.Metadata.Meta



-- | Read metadata described by given data type from file.
readMetadataEither :: forall a m. (IsMeta a, MonadIO m) => FilePath -> m (Either MetadataError a)
readMetadataEither path = liftIO $ do
  YAML.decodeFileEither path <&> \case
    Left  err  -> Left $ YamlError err
    Right json -> decodeMetadataEither json

-- | Read metadata described by given data type from file.
readMetadata :: forall a m. (IsMeta a, MonadIO m) => FilePath -> m a
readMetadata path = liftIO $ do
  YAML.decodeFileEither path >>= \case
    Left  err  -> throwIO err
    Right json -> case decodeMetadataEither json of
      Left  err  -> throw err
      Right a    -> pure a
