{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
-- |
-- Tools for defining concrete workflows. It contain both tools for
-- defining workflows themselves and @Flow@ wrappers.
module OKA.Flow.Tools
{-  ( -- * Interaction with store
    -- $store
    FlowOutput(..)
  , FlowInput(..)
  , AsMetaEncoded(..)
    -- * Argument passing
  , FlowArgument(..)
  , AsFlowOutput(..)
    -- * Standard workflow execution
  , metaFromStdin
  , runFlowArguments
  , flowArgumentsFromCLI
  , executeStdWorkflow
  , executeStdWorkflowOut
    -- * Building GHC programs
  , compileProgramGHC
  , defGhcOpts
    -- * External process
  , runExternalProcess
  , runExternalProcessNoMeta
  , withParametersInEnv
  , softKill
  ) -} where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson                   qualified as JSON
import Data.Aeson.Types             qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Monoid                  (Endo(..))
import Data.Functor
import System.Process.Typed
import System.FilePath              ((</>))
import System.IO.Temp
import System.IO                    (hClose)
import System.Environment           (getArgs)
import System.Posix.Signals         (signalProcess, sigINT)
import OKA.Metadata
import OKA.Flow.Core.Resources
import OKA.Flow.Core.Types
import OKA.Flow.Core.S
import OKA.Flow.Internal.Parser


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
  -- deriving FlowArgument via AsFlowOutput a

instance MetaEncoding a => FlowOutput (AsMetaEncoded a) where
  writeOutput dir (AsMetaEncoded a) =
    BL.writeFile (dir </> "data.json") $ JSON.encode $ metaToJson a

instance MetaEncoding a => FlowInput (AsMetaEncoded a) where
  readOutput dir = do
    bs <- BL.readFile (dir </> "data.json")
    case JSON.decode bs of
      Nothing -> error "Invalid JSON"
      Just js -> case JSON.parseEither parseMeta js of
        Left  e -> error $ "Cannot decode PErr: " ++ show e
        Right x -> pure (AsMetaEncoded x)

-- -- | Derive instance of 'FlowArgument' for instance of 'FlowInput'
-- newtype AsFlowOutput a = AsFlowOutput a

-- instance (FlowInput a) => FlowArgument (AsFlowOutput a) where
--   type AsRes (AsFlowOutput a) = Result a
--   parserFlowArguments = do
--     path <- consumeIO
--     liftIO $ AsFlowOutput <$> readOutput path

----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance FlowArgument () where
  type AsRes () = ()
  parseFlowArguments = \case
    Nil -> pure (pure ())
    _   -> Left "Expecting nil"
  parameterShape _   ()  = ()

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
-- Calling conventions
----------------------------------------------------------------


-- $standard_exe
--
-- Standard calling conventions for external process.
--
--  * JSON-encoded metadata is passed to process via stdin.
--
--  * Arguments are passed via command line serialized as S-expression
--
--  * Working directory is set to output directory

callStandardExe
  :: ParamFlow FilePath
  -> ProcessData
callStandardExe p = ProcessData
  { stdin   = Just $ JSON.encode $ encodeMetadata p.meta
  , env     = []
  , args    = sexpToArgs p.args
  , io      = id
  , workdir = p.out
  }

-- | Read metadata from stdin
metaFromStdin :: IsMeta a => IO a
metaFromStdin = do
  (BL.getContents <&> JSON.eitherDecode) >>= \case
    Left  e  -> error $ "Cannot read metadata: " ++ e
    Right js -> evaluate $ decodeMetadata js

loadParametersFromCLI :: FlowArgument a => IO a
loadParametersFromCLI = do
  args <- getArgs
  s    <- case sexpFromArgs args of
    Left  err -> error $ "loadFlowArguments: Cannot parse S-expression: " ++ err
    Right s   -> pure s
  case parseFlowArguments s of
    Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
    Right ioa -> ioa


----------------------------------------------------------------
-- Pass in enviroment
----------------------------------------------------------------

-- | Pass arguments in the environment
callInEnvironment
  :: ParamFlow FilePath
  -> ProcessData
callInEnvironment p = ProcessData
  { stdin   = Nothing
  , env     = undefined
  , args    = []
  , io      = undefined
  , workdir = p.out
  }

----------------------------------------------------------------
-- Conversion
----------------------------------------------------------------

sexpToArgs :: S FilePath -> [FilePath]
sexpToArgs = ($ []) . appEndo . go where
  go = \case
    Nil     -> cons "-"
    Atom  a -> cons ('!':a)
    Param p -> cons p
    S xs    -> cons "(" <> foldMap go xs <> cons ")"
  cons s = Endo (s:)

sexpFromArgs :: [FilePath] -> Either String (S FilePath)
sexpFromArgs [] = pure Nil
sexpFromArgs xs = runListParser parserS xs where
  parserS = consume >>= \case
    ""    -> fail "Empty parameter encountered"
    "("   -> S <$> parserL
    '!':a -> pure $ Atom  a
    p     -> pure $ Param p
  parserL =  eol
         <|> liftA2 (:) parserS parserL
  eol     = consume >>= \case
    ")" -> pure []
    _   -> fail "Expecting )"


----------------------------------------------------------------
-- Standard tools for writing executables
----------------------------------------------------------------


-- -- | Read arguments from using 'FlowArgument' type class.
-- runFlowArguments :: FlowArgument a => [FilePath] -> IO a
-- runFlowArguments paths = runListParserIO parserFlowArguments paths >>= \case
--   Left  e -> error $ "runFlowArguments: " ++ e
--   Right a -> pure a

-- -- | Parse standard arguments passed as command line parameters
-- flowArgumentsFromCLI :: FlowArgument a => IO (FilePath, a)
-- flowArgumentsFromCLI = getArgs >>= \case
--     []         -> error "flowArgumentsFromCLI: No output directory provided"
--     (out:args) -> do a <- runFlowArguments args
--                      pure (out,a)


-- -- | Execute workflow as a separate process using standard calling
-- --   conventions: metadata is written to stdin, output directory and
-- --   store paths are passed as command line parameters.
-- executeStdWorkflow
--   :: (IsMeta meta, FlowArgument a, FlowOutput b)
--   => (meta -> a -> IO b)
--   -> IO ()
-- executeStdWorkflow action = executeStdWorkflowOut $ \meta a out -> do
--   b <- action meta a
--   writeOutput out b

-- -- | Same as 'executeStdWorkflow' but workflow should write output directory by itself.
-- executeStdWorkflowOut
--   :: (IsMeta meta, FlowArgument a)
--   => (meta -> a -> FilePath -> IO ())
--   -> IO ()
-- executeStdWorkflowOut action = do
--   meta <- metaFromStdin
--   getArgs >>= \case
--     []         -> error "executeStdWorkflowOut: No output directory provided"
--     (out:args) -> do a <- runFlowArguments args
--                      action meta a out



-- ----------------------------------------------------------------
-- -- GHC compilation
-- ----------------------------------------------------------------

-- -- | Compile program with GHC. Uses 'LockGHC' to ensure that only one
-- --   compilation runs at a time.
-- compileProgramGHC
--   :: FilePath    -- ^ Path to Main module
--   -> [String]    -- ^ Options to pass to GHC
--   -> ResourceSet -- ^ Set of resources
--   -> IO ()
-- compileProgramGHC exe opts res
--   = withResources res LockGHC
--   $ withProcessWait_ ghc
--   $ \_ -> pure ()
--   where
--     ghc = proc "ghc" (exe:opts)

-- -- | Default GHC options
-- defGhcOpts :: [String]
-- defGhcOpts = [ "-O2"
--              , "-threaded"
--              , "-with-rtsopts=-T -A8m"
--              ]


-- ----------------------------------------------------------------
-- -- Run external processes
-- ----------------------------------------------------------------

-- -- | Run external process that adheres to standard calling
-- --   conventions: metadata is written to the stdin of a process and
-- --   parameters are passed as command line parameters.
-- runExternalProcess
--   :: FilePath   -- ^ Path to executable
--   -> Metadata   -- ^ Metadata to pass to process
--   -> [FilePath] -- ^ Parameter list. When called as part of a workflow
--                 --   absolute paths will be passed.
--   -> IO ()
-- runExternalProcess exe meta args = do
--   withProcessWait_ run $ \pid -> do
--     _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
--     pure ()
--   where
--     run = setStdin (byteStringInput $ JSON.encode $ encodeMetadata meta)
--         $ proc exe args

-- -- | Run external process that adheres to standard calling conventions
-- --   but don't pass metadata to it. Useful for phony targets.
-- runExternalProcessNoMeta
--   :: FilePath   -- ^ Path to executable
--   -> [FilePath] -- ^ Parameter list. When called as part of a workflow
--                 --   absolute paths will be passed.
--   -> IO ()
-- runExternalProcessNoMeta exe args = do
--   withProcessWait_ run $ \pid -> do
--     _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
--     pure ()
--   where
--     run = proc exe args


-- -- | Pass metadata parameters in the environment. Metadata is written
-- --   in temporary file which is deleted when callback returns.
-- --
-- --   Path to JSON encoded metadata will be passed in @OKA_META@
-- --   variable, absolute paths to parameters will be passed in
-- --   @OKA_ARG_#@ variables.
-- withParametersInEnv
--   :: Metadata                    -- ^ Metadata
--   -> [FilePath]                  -- ^ Parameters
--   -> ([(String,String)] -> IO a) -- ^ Callback
--   -> IO a
-- withParametersInEnv meta param action = do
--   withSystemTempFile "oka-flow-metadata-" $ \file_meta h -> do
--     BL.hPutStr h $ JSON.encode $ encodeMetadata meta
--     hClose h
--     action $ ("OKA_META", file_meta)
--            : [ ("OKA_ARG_" ++ show i, arg)
--              | (i,arg) <- [1::Int ..] `zip` param
--              ]
