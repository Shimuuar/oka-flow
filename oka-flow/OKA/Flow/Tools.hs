{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
-- |
-- Tools for defining concrete workflows. It contain both tools for
-- defining workflows themselves and @Flow@ wrappers.
module OKA.Flow.Tools
  ( -- * Interaction with store
    -- $store
    FlowOutput(..)
  , FlowInput(..)
  , Action(..)
  , Dataflow(..)
  , ParamFlow(..)
  , S(..)
    -- ** Argument reading
  , FlowArgument(..)
    -- ** Deriving via
  , AsMetaEncoded(..)
  , AsFlowInput(..)
    -- * Primitives for creating flows
  , liftWorkflow
  , liftPhony
  , HaskFlow(..)
  , HaskFlowIO(..)
  , HaskFlowPhony(..)
  , HaskFlowPhonyMeta(..)
    -- * Creating executables
  , metaFromStdin
  , loadParametersFromCLI
  ) where

import Control.Exception
import Control.Lens                 ((<&>),(^?))
import Data.Aeson                   qualified as JSON
import Data.Aeson.Types             qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Map.Strict              qualified as Map
import Data.Coerce
import Data.Foldable                (toList)
import Data.Traversable             (mapAccumL)
import System.FilePath              ((</>))
import System.Environment           (getArgs)

import GHC.Stack

import OKA.Metadata
import OKA.Flow.Core.Result
import OKA.Flow.Core.Types
import OKA.Flow.Core.Graph
import OKA.Flow.Core.S
import OKA.Flow.Core.Flow


----------------------------------------------------------------
-- Interaction with store
----------------------------------------------------------------

-- $store
--
-- Type class 'FlowOutput' and 'FlowInput' define simple standard
-- method of reading and writing data to store paths. If data type
-- implements both following law should hold:
--
-- > do writeOutput path a
-- >    a' <- readOutput path
-- >    a == a'


-- | Type class which describes how data type should be stored to
--   output.
class FlowOutput a where
  -- | Write output to the given directory.
  writeOutput :: FilePath -- ^ Output directory
              -> a        -- ^ Value to be saved
              -> IO ()

-- | Type class which describes how data type should be read from
--   output.
class FlowInput a where
  -- | Read output from given directory
  readOutput :: FilePath -- ^ Directory to read from
             -> IO a


-- | This type class describes how we parse and load data type of type
--   @a@ from S-expression containing path in store.
class (ToS (AsRes a)) => FlowArgument a where
  -- | 'Result' or tuple of 'Result's which represent this data type.
  type AsRes a
  -- | How data types should be parsed from list of absolute path to
  --   directories.
  parseFlowArguments
    :: (AResult -> FilePath) -- ^ Oracle for converting handles to absolute store path
    -> AsRes a               -- ^ Representation used in dataflow definition
    -> IO a



----------------------------------------------------------------
-- Deriving via
----------------------------------------------------------------

-- | Newtype wrapper for deriving 'FlowInput' and 'FlowOutput'
--   instance where data is store as JSON file @data.json@ encoded
--   using 'MetaEncoding' instance.
newtype AsMetaEncoded a = AsMetaEncoded a
  deriving FlowArgument via AsFlowInput a

instance MetaEncoding a => FlowOutput (AsMetaEncoded a) where
  writeOutput dir (AsMetaEncoded a) =
    BL.writeFile (dir </> "data.json") $ JSON.encode $ metaToJson a

instance MetaEncoding a => FlowInput (AsMetaEncoded a) where
  readOutput dir = do
    bs <- BL.readFile (dir </> "data.json")
    case JSON.decode bs of
      Nothing -> error "Invalid JSON"
      Just js -> case JSON.parseEither parseMeta js of
        Left  e -> error $ "Cannot decode metadata: " ++ show e
        Right x -> pure (AsMetaEncoded x)


-- | Derive instance of 'FlowArgument' for instance of 'FlowInput'
newtype AsFlowInput a = AsFlowInput a

instance (FlowInput a) => FlowArgument (AsFlowInput a) where
  type AsRes (AsFlowInput a) = Result a
  parseFlowArguments oracle (Result i)
    = AsFlowInput <$> readOutput (oracle i)


----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance FlowArgument () where
  type AsRes () = ()
  parseFlowArguments _ = pure

instance FlowArgument Bool where
  type AsRes Bool = Bool
  parseFlowArguments _ = pure

instance FlowArgument a => FlowArgument [a] where
  type AsRes [a] = [AsRes a]
  parseFlowArguments oracle = traverse (parseFlowArguments oracle)

instance FlowArgument a => FlowArgument (Maybe a) where
  type AsRes (Maybe a) = Maybe (AsRes a)
  parseFlowArguments oracle = traverse (parseFlowArguments oracle)

instance (FlowArgument a, FlowArgument b) => FlowArgument (a,b) where
  type AsRes (a,b) = (AsRes a, AsRes b)
  parseFlowArguments oracle (ra,rb) = do
    a <- parseFlowArguments oracle ra
    b <- parseFlowArguments oracle rb
    pure (a,b)

instance (FlowArgument a, FlowArgument b, FlowArgument c) => FlowArgument (a,b,c) where
  type AsRes (a,b,c) = (AsRes a, AsRes b, AsRes c)
  parseFlowArguments oracle (ra,rb,rc) = do
    a <- parseFlowArguments oracle ra
    b <- parseFlowArguments oracle rb
    c <- parseFlowArguments oracle rc
    pure (a,b,c)

instance (FlowArgument a, FlowArgument b, FlowArgument c, FlowArgument d
         ) => FlowArgument (a,b,c,d) where
  type AsRes (a,b,c,d) = (AsRes a, AsRes b, AsRes c, AsRes d)
  parseFlowArguments oracle (ra,rb,rc,rd) = do
    a <- parseFlowArguments oracle ra
    b <- parseFlowArguments oracle rb
    c <- parseFlowArguments oracle rc
    d <- parseFlowArguments oracle rd
    pure (a,b,c,d)



----------------------------------------------------------------
-- Lifting haskell function
--
-- FIXME: How to properly encode result of b? Is Result OK?
----------------------------------------------------------------

newtype HaskFlow meta a b = HaskFlow (meta -> a -> IO b)

instance (IsMeta meta, FlowArgument a, FlowOutput b
         ) => IsDataflow (HaskFlow meta a b) where
  runDataflow oracle (HaskFlow action) p = do
      meta <- case p.meta ^? metadata of
        Just m  -> pure m
        Nothing -> error $ "loadFlowArguments: Cannot get metadata"
      a <- case fromS p.args of
        Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
        Right asr -> parseFlowArguments oracle asr
      b <- action meta a
      case p.out of
        OutputRes out -> writeOutput out b


newtype HaskFlowIO meta a = HaskFlowIO (FilePath -> meta -> a -> IO ())

instance (IsMeta meta, FlowArgument a) => IsDataflow (HaskFlowIO meta a) where
  runDataflow oracle (HaskFlowIO action) p = do
    meta <- case p.meta ^? metadata of
      Just m  -> pure m
      Nothing -> error $ "loadFlowArguments: Cannot get metadata"
    a <- case fromS p.args of
      Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
      Right asr -> parseFlowArguments oracle asr
    case p.out of
      OutputRes out -> action out meta a

newtype HaskFlowPhony meta a = HaskFlowPhony (meta -> a -> IO ())

instance (IsMeta meta, FlowArgument a) => IsPhony (HaskFlowPhony meta a) where
  runPhony oracle (HaskFlowPhony action) p = do
    meta <- case p.meta ^? metadata of
      Just m  -> pure m
      Nothing -> error $ "loadFlowArguments: Cannot get metadata"
    a <- case fromS p.args of
      Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
      Right asr -> parseFlowArguments oracle asr
    action meta a


newtype HaskFlowPhonyMeta a = HaskFlowPhonyMeta (Metadata -> a -> IO ())

instance (FlowArgument a) => IsPhony (HaskFlowPhonyMeta a) where
  runPhony oracle (HaskFlowPhonyMeta action) p = do
    a <- case fromS p.args of
      Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
      Right asr -> parseFlowArguments oracle asr
    action p.meta a



----------------------------------------------------------------
-- Defining subprocesses
----------------------------------------------------------------

-- | Read metadata from stdin
metaFromStdin :: (IsMeta a, HasCallStack) => IO a
metaFromStdin = do
  (BL.getContents <&> JSON.eitherDecode) >>= \case
    Left  e  -> error $ "Cannot read metadata: " ++ e
    Right js -> evaluate $ decodeMetadata js

-- | Load arguments from CLI parameters
loadParametersFromCLI :: (HasCallStack) => FlowArgument a => IO a
loadParametersFromCLI = do
  args <- getArgs
  s    <- case sexpFromArgs args of
    Left  err -> error $ "loadFlowArguments: Cannot parse S-expression: " ++ err
    Right s   -> pure s
  let (sa, oracle) = fakeOracle s
  case fromS sa of
    Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
    Right asr -> parseFlowArguments oracle asr

-- Hack where we artificially convert S FilePath to pair of S AResult
-- and oracle function
fakeOracle :: S FilePath -> (S AResult, AResult -> FilePath)
fakeOracle s = (s_ares, oracle)
  where
    ares0       = AResult (FunID 0)
    (_, s_ares) = mapAccumL (\r _ -> (succAResult r, r)) ares0 s
    path_map    = Map.fromList $ zip (iterate succAResult ares0) (toList s)
    oracle r    = path_map Map.! r
    --
    succAResult :: AResult -> AResult
    succAResult = coerce (+ (1::Int))
