-- |
-- Simple Merkle tree for computing hash of flow.
module OKA.Flow.Core.Merkle
  ( Merkle(..)
  , hashMerkle
  , hashMeta
  , hashS
  ) where

import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.Aeson.KeyMap            qualified as KM
import Data.Aeson.Key               (toText)
import Data.ByteString.Builder      qualified as BB
import Data.Coerce
import Data.List                    (sortOn,intersperse)
import Data.Vector                  qualified as V
import Data.Text.Encoding           qualified as T
import GHC.Stack

import OKA.Metadata.Meta
import OKA.Flow.Core.Types
import OKA.Flow.Core.S


-- | Data type with
data Merkle
  = MerkleBranch [Merkle]
    -- ^ Branch of Merkle tree
  | MerkleName   String
  | MerkleS      (S StorePath)
    -- ^ Leaf node with parameters passed to dataflow
  | MerkleMeta   Metadata
    -- ^ Leaf node with metadata
  | MerkleExt    [([JSON.Key], StorePath)]
    -- ^ List of external metadata load from output of another dataflow

hashMerkle :: Merkle -> Hash
hashMerkle = \case
  MerkleBranch xs -> hashBranch xs
  MerkleName   nm -> hashBuilder "?NAME?" $ BB.stringUtf8 nm
  MerkleS      s  -> hashS      s
  MerkleMeta   m  -> hashMeta   m
  MerkleExt    xs -> hashExt    xs


hashBuilder :: String -> BB.Builder -> Hash
{-# INLINE hashBuilder #-}
hashBuilder prefix builder
  = Hash $ SHA1.hashlazy $ BB.toLazyByteString $ BB.string7 prefix <> builder


hashBranch :: [Merkle] -> Hash
hashBranch = hashBuilder "?BRANCH?" . foldMap (builderHash . hashMerkle)


-- | Compute hash of S expression 
hashS :: S StorePath -> Hash
hashS s0
  = hashBuilder "?ARGS?" $ go s0
  where
    go = \case
      Param (StorePath _ (Hash h))
        -> BB.char7 '/' <> BB.byteString h
      Atom  a -> BB.char7 ':' <> BB.string8 a
      Nil     -> BB.char7 '-'
      S ss    -> BB.char7 '('
              <> mconcat (intersperse (BB.char7 ',') (go <$> ss))
              <> BB.char7 ')'


-- | Compute hash of metadata
hashMeta :: (HasCallStack) => Metadata -> Hash
hashMeta
  = hashBuilder "?META?"
  . JSONB.fromEncoding
  . encodeToBuilder
  . encodeMetadata

encodeToBuilder :: (HasCallStack) => JSON.Value -> JSONB.Encoding
encodeToBuilder JSON.Null       = JSONB.null_
encodeToBuilder (JSON.Bool b)   = JSONB.bool b
encodeToBuilder (JSON.Number n) = JSONB.scientific n
encodeToBuilder (JSON.String s) = JSONB.text s
encodeToBuilder (JSON.Array v)  = jsArray v
encodeToBuilder (JSON.Object m) = JSONB.dict JSONB.text encodeToBuilder
  (\step z m0 -> foldr (\(k,v) a -> step (toText k) v a) z $ sortOn fst $ KM.toList m0)
  m

jsArray :: (HasCallStack) => V.Vector JSON.Value -> JSONB.Encoding
jsArray v
  | V.null v  = JSONB.emptyArray_
  | otherwise = JSONB.wrapArray
              $  encodeToBuilder (V.unsafeHead v)
             <@> V.foldr withComma (JSONB.Encoding mempty) (V.unsafeTail v)
  where
    withComma a z = JSONB.comma <@> encodeToBuilder a <@> z
    JSONB.Encoding e1 <@> JSONB.Encoding e2 = JSONB.Encoding (e1 <> e2)


-- | Hash of external metadata. We sort it using @metaLocation@ in
--   order to provide canonicalization.
hashExt :: [([JSON.Key], StorePath)] -> Hash
hashExt = hashBuilder "?EXT?" . foldMap hash . sortOn fst
  where
    hash (loc, StorePath _ h)
      = foldMap (T.encodeUtf8Builder . toText) loc
     <> builderHash h

builderHash :: Hash -> BB.Builder
builderHash = coerce BB.byteString
