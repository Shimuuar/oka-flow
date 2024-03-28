{-# LANGUAGE DerivingVia      #-}
{-# LANGUAGE FlexibleContexts #-}
-- |
-- Tools for defining concrete workflows.
module OKA.Flow.Tools
  ( -- * Executable implementation
    metaFromStdin
    -- ** Output serialization
  , FlowOutput(..)
  , AsMetaEncoded(..)
    -- ** Command line arguments
  , FlowArgument(..)
  , parseFlowArguments
  , parseSingleArgument
  , AsFlowOutput(..)
    -- * Resources
  , LockGHC(..)
  , compileProgramGHC
  , defGhcOpts
  , LockLMDB(..)
  , LockCoreCPU(..)
  , LockMemGB(..)
    -- * External process
  , runExternalProcess
  , runExternalProcessNoMeta
  , runJupyter
  ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Strict
import Data.Aeson                   qualified as JSON
import Data.Aeson.Types             qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Functor
import System.Process.Typed
import System.Directory
import System.FilePath              ((</>))
import System.IO.Temp
import System.Environment           (getEnvironment, getArgs)
import System.Posix.Signals         (signalProcess, sigINT)
import System.Process               (getPid)
import OKA.Metadata
import OKA.Flow.Types

----------------------------------------------------------------
-- Standard tools for writing executables
----------------------------------------------------------------

-- | Read metadata from stdin
metaFromStdin :: IsMeta a => IO a
metaFromStdin = do
  (BL.getContents <&> JSON.eitherDecode) >>= \case
    Left  e  -> error $ "Cannot read metadata: " ++ e
    Right js -> evaluate $ decodeMetadata js


-- | Type class for serialization of output to store
class FlowOutput a where
  -- | Write output to the given directory.
  writeOutput :: FilePath -> a -> IO ()
  -- | Read output from given directory
  readOutput :: FilePath -> IO a


-- | Save value as a JSON encoded with 'MetaEncoding' in @data.json@ file.
newtype AsMetaEncoded a = AsMetaEncoded a
  deriving FlowArgument via AsFlowOutput (AsMetaEncoded a)

instance MetaEncoding a => FlowOutput (AsMetaEncoded a) where
  writeOutput dir (AsMetaEncoded a) =
    BL.writeFile (dir </> "data.json") $ JSON.encode $ metaToJson a
  readOutput dir = do
    bs <- BL.readFile (dir </> "data.json")
    case JSON.decode bs of
      Nothing -> error "Invalid JSON"
      Just js -> case JSON.parseEither parseMeta js of
        Left  e -> error $ "Cannot decode PErr: " ++ show e
        Right x -> pure (AsMetaEncoded x)


-- | Type class for decoding arguments that are passed on command line
--   haskell data types.
class FlowArgument a where
  parserFlowArguments :: StateT [FilePath] (ExceptT String IO) a

-- | Parse command line arguments following flow's conventions: first
--   one is output directory rest are arguments.
parseFlowArguments :: FlowArgument a => IO (FilePath,a)
parseFlowArguments = getArgs >>= \case
  [] -> error "parseFlowArguments: No output directory provided"
  (out:paths) -> runExceptT (runStateT parserFlowArguments paths) >>= \case
    Left  e      -> error $ "parseFlowArguments: " ++ e
    Right (a,[]) -> pure (out,a)
    Right (_,_)  -> error "parseFlowArguments: not all inputs are consumed"

-- | Parse single input.
parseSingleArgument :: StateT [FilePath] (ExceptT String IO) FilePath
parseSingleArgument = do
  get >>= \case
    []   -> throwError "Not enough inputs"
    s:ss -> s <$ put ss

instance FlowArgument () where
  parserFlowArguments = pure ()
instance (FlowArgument a, FlowArgument b) => FlowArgument (a,b) where
  parserFlowArguments = (,) <$> parserFlowArguments <*> parserFlowArguments
instance (FlowArgument a, FlowArgument b, FlowArgument c) => FlowArgument (a,b,c) where
  parserFlowArguments = (,,) <$> parserFlowArguments <*> parserFlowArguments <*> parserFlowArguments

-- | Derive instance of 'FlowArgument' for instance of 'FlowOutput'
newtype AsFlowOutput a = AsFlowOutput a

instance FlowOutput a => FlowArgument (AsFlowOutput a) where
  parserFlowArguments = do
    path <- parseSingleArgument
    a    <- liftIO $ readOutput path
    pure $ AsFlowOutput a



----------------------------------------------------------------
-- GHC compilation
----------------------------------------------------------------

-- | We want to have one concurrent build. This data type provides mutex
data LockGHC = LockGHC 
  deriving stock (Show,Eq)
  deriving Resource via ResAsMutex LockGHC

-- | Compile program with GHC. Uses 'LockGHC' to ensure that only one
--   compilation runs at a time.
compileProgramGHC
  :: FilePath    -- ^ Path to Main module
  -> [String]    -- ^ Options to pass to GHC
  -> ResourceSet -- ^ Set of resources
  -> IO ()
compileProgramGHC exe opts res
  = withResources res LockGHC
  $ withProcessWait_ ghc
  $ \_ -> pure ()
  where
    ghc = proc "ghc" (exe:opts)

-- | Default GHC options
defGhcOpts :: [String]
defGhcOpts = [ "-O2"
             , "-threaded"
             , "-with-rtsopts=-T -A8m"
             ]



-- | We want to restrict number of simultaneous programs which connect to DB
newtype LockLMDB = LockLMDB Int
  deriving stock (Show,Eq)
  deriving Resource via ResAsCounter LockLMDB

newtype LockCoreCPU = LockCoreCPU Int
  deriving stock (Show,Eq)
  deriving Resource via ResAsCounter LockCoreCPU

-- | How much memory flow is expected to use in GB
newtype LockMemGB = LockMemGB Int
  deriving stock (Show,Eq)
  deriving Resource via ResAsCounter LockMemGB



----------------------------------------------------------------
-- Run external processes
----------------------------------------------------------------

-- | Run external process that adheres to standard calling conventions
runExternalProcess
  :: FilePath   -- ^ Path to executable
  -> Metadata   -- ^ Metadata to pass to process
  -> [FilePath] -- ^ Parameter list
  -> IO ()
runExternalProcess exe meta args = do
  withProcessWait_ run $ \pid -> do
    _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
    pure ()
  where
    run = setStdin (byteStringInput $ JSON.encode $ encodeMetadataDyn meta)
        $ proc exe args

-- | Run external process that adheres to standard calling conventions
runExternalProcessNoMeta
  :: FilePath   -- ^ Path to executable
  -> [FilePath] -- ^ Parameter list
  -> IO ()
runExternalProcessNoMeta exe args = do
  withProcessWait_ run $ \pid -> do
    _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
    pure ()
  where
    run = proc exe args

-- Kill process but allow it to die gracefully by sending SIGINT
-- first. GHC install handler for it but not for SIGTERM
softKill :: Process stdin stdout stderr -> IO ()
softKill p = getPid (unsafeProcessHandle p) >>= \case
  Nothing  -> pure ()
  Just pid -> do
    delay <- registerDelay 1_000_000
    signalProcess sigINT pid
    atomically $  (check =<< readTVar delay)
              <|> (void $ waitExitCodeSTM p)


-- | Run jupyter notebook as an external process
runJupyter
  :: FilePath   -- ^ Notebook name
  -> Metadata   -- ^ Metadata
  -> [FilePath] -- ^ Parameters
  -> IO ()
-- FIXME: We need mutex although not badly. No reason to run two
--        notebooks concurrently
runJupyter notebook meta param = do
  withSystemTempDirectory "oka-juju" $ \tmp -> do
    let dir_config  = tmp </> "config"
        dir_data    = tmp </> "data"
        file_meta   = tmp </> "meta.json"
    createDirectory dir_config
    createDirectory dir_data
    cwd <- getCurrentDirectory
    BL.writeFile file_meta $ JSON.encode $ encodeMetadataDyn meta
    --
    env <- getEnvironment
    let run = setEnv ( ("JUPYTER_DATA_DIR",   dir_data)
                     : ("JUPYTER_CONFIG_DIR", dir_config)
                     : ("OKA_META", file_meta)
                     : [ ("OKA_ARG_" ++ show i, cwd </> arg)
                       | (i,arg) <- [1::Int ..] `zip` param
                       ]
                     ++ env
                     )
            $ proc "jupyter" [ "notebook" , notebook
                             , "--browser", "chromium"
                             ]
    withProcessWait_ run $ \_ -> pure ()
