{-# LANGUAGE RecordWildCards #-}
-- |
module OKA.Flow.Core.CallExe
  ( -- * Calling conventions
    CallExe(..)
  , ccModifyArgs
  , ccSetArgs
  , ccAddEnv
  , ccStdin
  , ccStdout
  , ccStdinBS
  , ccStdinBL
  , ccStdinText
  , ccStdinString
  , Stdin(..)
  , Stdout(..)
  , CmdArg(..)
  , ProcessData(..)
    -- * Standard calling conventions
  , callStandardExe
  , callInEnvironment
  , callViaArgList
    -- * Spawning process
  , toTypedProcess
  , startSubprocessAndWait
  ) where


import Control.Applicative
import Control.Monad
import Control.Exception
import Control.Concurrent.STM
import Data.Aeson             qualified as JSON
import Data.ByteString        qualified as BS
import Data.ByteString.Lazy   qualified as BL
import Data.String
import Data.Text              (Text)
import Data.Text              qualified as T
import Data.Text.Encoding     qualified as T
import Data.Traversable
import System.FilePath        ((</>),pathSeparator,isAbsolute)
import System.Directory       (makeAbsolute)
import System.Environment     (getEnvironment)
import System.Process.Typed   (ProcessConfig,Process,ExitCode(..),proc,setStdin,setStdout,setWorkingDir,
                               setEnv,nullStream,byteStringInput,useHandleClose,withProcessWait_,
                               waitExitCodeSTM,getPid
                              )
import System.IO              (openFile,hClose,hPutStr,IOMode(..))
import System.Posix.Signals   (signalProcess, sigINT)
import System.IO.Temp
import GHC.Stack

import OKA.Flow.Core.Types    (ParamFlow(..))
import OKA.Flow.Core.S
import OKA.Metadata



----------------------------------------------------------------
-- Calling conventions definition
----------------------------------------------------------------

-- | Bracket for passing parameters to a subprocess
newtype CallExe = CallExe (forall a. (ProcessData -> IO a) -> (ParamFlow FilePath -> IO a))

mapCallExe :: (ProcessData -> ProcessData) -> CallExe -> CallExe
mapCallExe f (CallExe call) = CallExe $ \action p ->
  call (action . f) p

-- | Modify argument list of a program
ccModifyArgs :: ([CmdArg] -> [CmdArg]) -> CallExe -> CallExe
ccModifyArgs fun = mapCallExe $ \ProcessData{..} -> ProcessData{args = fun args, ..}

-- | Prepend arguments to list being passed to flow. They are not
--   modified.
ccSetArgs :: [CmdArg] -> CallExe -> CallExe
ccSetArgs xs = mapCallExe $ \ProcessData{..} -> ProcessData{args = xs, ..}

-- | Add values to environment variables
ccAddEnv :: [(String,String)] -> CallExe -> CallExe
ccAddEnv xs = mapCallExe $ \ProcessData{..} -> ProcessData{env = xs ++ env, ..}

ccStdin :: Stdin -> CallExe -> CallExe
ccStdin dat = mapCallExe $ \ProcessData{..} -> ProcessData{stdin = dat, ..}

ccStdout :: Stdout -> CallExe -> CallExe
ccStdout dat = mapCallExe $ \ProcessData{..} -> ProcessData{stdout = dat, ..}

-- | Pass strict bytestring to process's stdin.
ccStdinBL :: BL.ByteString -> CallExe -> CallExe
ccStdinBL dat = ccStdin (StdinBS dat)

-- | Pass lazy bytestring to process's stdin.
ccStdinBS :: BS.ByteString -> CallExe -> CallExe
ccStdinBS dat = ccStdinBL (BL.fromStrict dat)

-- | Pass strict text to process's stdin. It will be UTF8 encoded.
ccStdinText :: Text -> CallExe -> CallExe
ccStdinText dat = ccStdinBS (T.encodeUtf8 dat)

-- | Pass string to process's stdin. It will be UTF8 encoded.
ccStdinString :: String -> CallExe -> CallExe
ccStdinString dat = ccStdinText (T.pack dat)



-- | What to do with stdin of subprocess
data Stdin
  = DevNullIn                 -- ^ Close @stdin@ of  subprocess
  | StdinBS    !BL.ByteString -- ^ Write lazy bytestring to stdin
  | StdinFileA !FilePath      -- ^ Read stdin from file. File path is
                              --   relative to workdir of dataflow manager
  | StdinFileO !FilePath      -- ^ Read stdin from file. File path is relative to
                              --   output directory.
  deriving stock (Show,Eq)

-- | What to do with stdout of subprocess
data Stdout
  = Inherit              -- ^ Inherit @stdout@
  | DevNullOut           -- ^ Close @std
  | StdoutFile !FilePath -- ^ Write stdout to file
  deriving stock (Show,Eq)

-- | Command line argument. We need to distinguish between relative
--   path and command line arguments. We need to convert former to
--   absolute paths since we don't know working dir of subprocess.
--   Note that paths from store are already absolute there's little
--   reason to represent them as @Path@.
data CmdArg
  = CmdArg !FilePath -- ^ Command line argument which is
  | PathA  !FilePath -- ^ Path relative to working directory of workflow manager.
  | PathO  !FilePath -- ^ Path relative to working directory of store
                     --   output of corresponding subprocess.
  deriving stock (Show,Eq)

instance IsString CmdArg where
  fromString = CmdArg


-- | Data for calling external process
data ProcessData = ProcessData
  { executable :: !FilePath         -- ^ Executable name. If it contains path separators
                                    --   it will be converted to absolute path relative
  , stdin      :: !Stdin            -- ^ Data to pass to stdin
  , stdout     :: !Stdout           -- ^ Stdout treatment
  , env        :: [(String,String)] -- ^ Data for adding to process environment
  , args       :: [CmdArg]          -- ^ Arguments for a process
  , workdir    :: !(Maybe FilePath) -- ^ Working directory for subprocess.
  }



----------------------------------------------------------------
-- Standard calling conventions
----------------------------------------------------------------

-- | Standard calling conventions for external process.
--
--  * JSON-encoded metadata is passed to process via stdin.
--
--  * Arguments are passed via command line serialized as S-expression
--
--  * Working directory is set to output directory
callStandardExe
  :: (HasCallStack)
  => FilePath
  -> ParamFlow FilePath
  -> (ProcessData -> IO a)
  -> IO a
callStandardExe exe p action = action ProcessData
  { executable = exe
  , stdin      = StdinBS $ JSON.encode $ encodeMetadata p.meta
  , stdout     = Inherit
  , env        = []
  , args       = CmdArg <$> sexpToArgs p.args
  , workdir    = p.out
  }


-- | Pass arguments in the environment.
--
-- * Metadata is written in temporary file. It's stored in
--   @OKA_META@ environment variable.
--
-- * Arguments are written into temporary file one value on each line.
--   File name is stored in @OKA_ARGS@ environment variable.
--
-- * Working directory is set to output directory. Additionally it's
--   stored in @OKA_OUT@ environment variable.
callInEnvironment
  :: (HasCallStack)
  => FilePath
  -> CallExe 
callInEnvironment exe = CallExe $ \action p ->
  withSystemTempFile "oka-flow-metadata-" $ \file_meta h_meta ->
  withSystemTempFile "oka-flow-metadata-" $ \file_args h_args -> do
    -- Write metadata to temporary file
    BL.hPutStr h_meta $ JSON.encode $ encodeMetadata p.meta
    hClose h_meta
    -- Write arguments to temporary file
    hPutStr h_args $ unlines $ sexpToArgs p.args
    hClose h_args
    -- Populate environment
    let env = case p.out of
                Nothing -> id
                Just d  -> (("OKA_OUT", d):)
            $ ("OKA_META", file_meta)
            : ("OKA_ARGS", file_args)
            : []
    action ProcessData
      { executable = exe
      , stdin      = DevNullIn
      , stdout     = Inherit
      , env        = env
      , args       = []
      , workdir    = p.out
      }


-- | Call executable where all parameters are passing via command line
--   parameters.
--
--  * Metadata is discarded
--
--  * Working dir is set to output directory
callViaArgList
  :: (HasCallStack)
  => FilePath                 -- ^ Executable
  -> (S FilePath -> [CmdArg]) -- ^ How to transform list of store pathes
  -> CallExe
callViaArgList exe fun = CallExe $ \action p -> action $ ProcessData
  { executable = exe
  , stdin      = DevNullIn
  , stdout     = Inherit
  , env        = []
  , args       = fun p.args
  , workdir    = p.out
  }



----------------------------------------------------------------
-- Running executables
----------------------------------------------------------------

-- | Convert dictionary into data structure for calling external
--   process using @typed-process@
toTypedProcess
  :: (HasCallStack)
  => ProcessData -- ^ Parameters
  -> IO (ProcessConfig () () ())
toTypedProcess process = do
  -- Executable is searched relative to the working directory of
  -- process. We need to convert executable to absolute path when
  -- setting working dir. At the same we don't want to convert to
  -- absolute path executables from PATH.
  exe <- if
    | pathSeparator `elem` process.executable -> makeAbsolute process.executable
    | otherwise                               -> pure process.executable
  env <- case process.env of
    [] -> pure []
    es -> do env <- getEnvironment
             pure $ env ++ es
  --
  args <- for process.args $ \case
    CmdArg p -> pure p
    PathA  p -> makeAbsolute p
    PathO  p -> case process.workdir of
      Nothing -> error "Workdir for subprocess is not set. Path relative to output is not defined"
      Just _  -> pure p
  -- Normalization of path relative to workdir of subprocess. Note!
  -- Its workdir is different from ours
  let normalizePathO (isAbsolute -> True)
        = error "Path relative to workdir couldn't be absolute"
      normalizePathO p = case process.workdir of
        Nothing -> error "Workdir for subprocess is not set. Path relative to output is not defined"
        Just wd -> makeAbsolute $ wd </> p
  let set_stdin p = case process.stdin of
        DevNullIn       -> pure $ setStdin nullStream           p
        StdinBS    bs   -> pure $ setStdin (byteStringInput bs) p
        StdinFileA path -> do path' <- makeAbsolute path
                              h     <- openFile path' ReadMode
                              pure $ setStdin (useHandleClose h) p
        StdinFileO path -> do path' <- normalizePathO path
                              h     <- openFile path' ReadMode
                              pure $ setStdin (useHandleClose h) p
  let set_stdout p = case process.stdout of
        Inherit         -> pure p
        DevNullOut      -> pure $ setStdout nullStream p
        StdoutFile path -> do path' <- normalizePathO path
                              h     <- openFile path' WriteMode
                              pure $ setStdout (useHandleClose h) p
  id $ set_stdout <=< set_stdin
     $ case process.workdir of
         Nothing -> id
         Just p  -> setWorkingDir p
     $ case env of
         [] -> id
         _  -> setEnv env
     $ proc exe args

-- | Create subprocess and wait for its completions. If execution is
--   interrupted subprocess is killed. If process exits with nonzero
--   exit code exception is raised.
startSubprocessAndWait
  :: (HasCallStack)
  => CallExe            -- ^ Calling conventions
  -> ParamFlow FilePath -- ^ Parameters
  -> IO ()
startSubprocessAndWait (CallExe call) param =
  flip call param $ \process -> do
    run <- toTypedProcess process
    withProcessWait_ run $ \pid -> do
      r <- atomically (waitExitCodeSTM pid) `onException` softKill pid
      case r of
        ExitSuccess   -> pure ()
        ExitFailure{} -> throwIO r

-- Kill process but allow it to die gracefully by sending SIGINT
-- first. GHC install handler for it but not for SIGTERM
softKill :: (HasCallStack) => Process stdin stdout stderr -> IO ()
softKill p = getPid p >>= \case
  Nothing  -> pure ()
  Just pid -> do
    delay <- registerDelay 1_000_000
    signalProcess sigINT pid
    atomically $  (check =<< readTVar delay)
              <|> (void $ waitExitCodeSTM p)
