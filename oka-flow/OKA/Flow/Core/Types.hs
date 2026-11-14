-- |
-- Basic data types sued for definition of dataflow graph.
module OKA.Flow.Core.Types
  ( -- * Dataflow graph handles
    Result(..)
  , Phony(..)
  , AResult
  , APhony
    -- * Store path
  , Hash(..)
  , hashFromString
  , hashFromText
  , StorePath(..)
  , storePath
    -- * Flow parameters
  , ParamFlow(..)
  ) where

import Data.Aeson             qualified as JSON
import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Base16 qualified as Base16
import Data.Text              qualified as T
import Data.Text.Encoding     qualified as T
import System.FilePath        ((</>))

import OKA.Metadata
import OKA.Flow.Core.S
import OKA.Flow.Core.Result



----------------------------------------------------------------
-- Paths in store
----------------------------------------------------------------

-- | SHA1 hash
newtype Hash = Hash ByteString
  deriving newtype (Eq,Ord)

instance Show Hash where
  show (Hash hash) = show $ BC8.unpack $ Base16.encode hash

hashFromString :: FilePath -> Maybe Hash
hashFromString = hashFromText . T.pack

hashFromText :: T.Text -> Maybe Hash
hashFromText b16
  | Right hash <- Base16.decode (T.encodeUtf8 b16)
  , BC8.length hash == 20
  = Just (Hash hash)
  | otherwise
  = Nothing

-- | Path in nix-like storage.
data StorePath = StorePath
  { name :: String
  , hash :: Hash
  }
  deriving stock (Show,Eq,Ord)

instance JSON.ToJSON StorePath where
  toJSON = JSON.toJSON . storePath

instance JSON.FromJSON StorePath where
  parseJSON = JSON.withText "StorePath" $ \str ->
    case T.split (=='/') str of
      [path,hash_b16] -> do
        hash <- JSON.parseJSON $ JSON.String hash_b16
        pure $ StorePath (T.unpack path) hash
      _     -> fail "Invalid StorePath"

instance JSON.ToJSON Hash where
  toJSON (Hash hash) = JSON.toJSON $ BC8.unpack $ Base16.encode hash
instance JSON.FromJSON Hash where
  parseJSON = JSON.withText "Hash" $ \hash_b16 ->
    case hashFromText hash_b16 of
      Just hash -> pure hash
      Nothing   -> fail "Hash: invalid hash"


-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm </> BC8.unpack (Base16.encode hash)

----------------------------------------------------------------
-- Calling Flows
----------------------------------------------------------------

-- | Parameters for a standard flow
data ParamFlow a = ParamFlow
  { meta :: Metadata
  , args :: S a
  , out  :: Maybe a
  }
  deriving stock (Functor,Foldable,Traversable)
