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
import System.FilePath              ((</>))
import System.Environment           (getArgs)

import GHC.Stack

import OKA.Metadata
import OKA.Flow.Core.Resources
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
  parseFlowArguments :: S FilePath -> Either String (IO a)
  -- |
  parameterShape :: (forall x. Result x) -> a -> AsRes a



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
  parseFlowArguments = \case
    Param p -> Right $ AsFlowInput <$> readOutput p
    _       -> Left "Expecting single parameter"
  parameterShape x _ = x


----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance FlowArgument () where
  type AsRes () = ()
  parseFlowArguments = \case
    Nil -> pure (pure ())
    _   -> Left "Expecting nil"
  parameterShape _   ()  = ()

instance FlowArgument Bool where
  type AsRes Bool = Bool
  parseFlowArguments = \case
    Atom "True"  -> pure $ pure True
    Atom "False" -> pure $ pure False
    _            -> Left "Cannot parse boolean"
  parameterShape _ = id

instance FlowArgument a => FlowArgument [a] where
  type AsRes [a] = [AsRes a]
  parseFlowArguments = \case
    S xs -> do
      as <- traverse parseFlowArguments xs
      pure $ sequence as
    _ -> Left "Expecting list"
  parameterShape p   xs = parameterShape p <$> xs

instance FlowArgument a => FlowArgument (Maybe a) where
  type AsRes (Maybe a) = Maybe (AsRes a)
  parseFlowArguments = \case
    Nil -> pure (pure Nothing)
    s   -> do a <- parseFlowArguments s
              pure $ Just <$> a
  parameterShape p xs = parameterShape p <$> xs

instance (FlowArgument a, FlowArgument b) => FlowArgument (a,b) where
  type AsRes (a,b) = (AsRes a, AsRes b)
  parseFlowArguments = \case
    S [sa,sb] -> do
      ioa <- parseFlowArguments sa
      iob <- parseFlowArguments sb
      pure $ (,) <$> ioa <*> iob
    _ -> Left "Expecting 2-element list"
  parameterShape p (a,b) = (parameterShape p a, parameterShape p b)

instance (FlowArgument a, FlowArgument b, FlowArgument c) => FlowArgument (a,b,c) where
  type AsRes (a,b,c) = (AsRes a, AsRes b, AsRes c)
  parseFlowArguments = \case
    S [sa,sb,sc] -> do
      ioa <- parseFlowArguments sa
      iob <- parseFlowArguments sb
      ioc <- parseFlowArguments sc
      pure $ (,,) <$> ioa <*> iob <*> ioc
    _ -> Left "Expecting 3-element list"
  parameterShape p (a,b,c) = ( parameterShape p a
                             , parameterShape p b
                             , parameterShape p c)

instance (FlowArgument a, FlowArgument b, FlowArgument c, FlowArgument d
         ) => FlowArgument (a,b,c,d) where
  type AsRes (a,b,c,d) = (AsRes a, AsRes b, AsRes c, AsRes d)
  parseFlowArguments = \case
    S [sa,sb,sc,sd] -> do
      ioa <- parseFlowArguments sa
      iob <- parseFlowArguments sb
      ioc <- parseFlowArguments sc
      iod <- parseFlowArguments sd
      pure $ (,,,) <$> ioa <*> iob <*> ioc <*> iod
    _ -> Left "Expecting 3-element list"
  parameterShape p (a,b,c,d) = ( parameterShape p a
                               , parameterShape p b
                               , parameterShape p c
                               , parameterShape p d)



----------------------------------------------------------------
-- Lifting haskell function
--
-- FIXME: How to properly encode result of b? Is Result OK?
----------------------------------------------------------------

newtype HaskFlow meta a b = HaskFlow (meta -> a -> IO b)

instance (IsMeta meta, FlowArgument a, FlowOutput b
         ) => IsDataflow (HaskFlow meta a b) where
  runDataflow (HaskFlow action) p = do
      meta <- case p.meta ^? metadata of
        Just m  -> pure m
        Nothing -> error $ "loadFlowArguments: Cannot get metadata"
      a    <- case parseFlowArguments p.args of
        Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
        Right ioa -> ioa
      b <- action meta a
      case p.out of
        OutputRes out -> writeOutput out b


newtype HaskFlowIO meta a = HaskFlowIO (FilePath -> meta -> a -> IO ())

instance (IsMeta meta, FlowArgument a) => IsDataflow (HaskFlowIO meta a) where
  runDataflow (HaskFlowIO action) p = do
    meta <- case p.meta ^? metadata of
      Just m  -> pure m
      Nothing -> error $ "loadFlowArguments: Cannot get metadata"
    a    <- case parseFlowArguments p.args of
      Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
      Right ioa -> ioa
    case p.out of
      OutputRes out -> action out meta a

newtype HaskFlowPhony meta a = HaskFlowPhony (meta -> a -> IO ())

instance (IsMeta meta, FlowArgument a) => IsPhony (HaskFlowPhony meta a) where
  runPhony (HaskFlowPhony action) p = do
    meta <- case p.meta ^? metadata of
      Just m  -> pure m
      Nothing -> error $ "loadFlowArguments: Cannot get metadata"
    a    <- case parseFlowArguments p.args of
      Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
      Right ioa -> ioa
    action meta a


newtype HaskFlowPhonyMeta a = HaskFlowPhonyMeta (Metadata -> a -> IO ())

instance (FlowArgument a) => IsPhony (HaskFlowPhonyMeta a) where
  runPhony (HaskFlowPhonyMeta action) p = do
    a <- case parseFlowArguments p.args of
      Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
      Right ioa -> ioa
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
  case parseFlowArguments s of
    Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
    Right ioa -> ioa
