-- |
module OKA.Flow.Core.Types
  ( -- * Dataflow graph handles
    FunID(..)
  , Result(..)
    -- * Store path
  , Hash(..)
  , StorePath(..)
  , storePath
    -- * Flow parameters
  , ParamFlow(..)
  , ProcessData(..)
  , toTypedProcess
  ) where

import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Lazy   qualified as BL
import Data.ByteString.Base16 qualified as Base16
import System.FilePath        ((</>))
import System.Environment     (getEnvironment)
import System.Process.Typed

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

-- | Path in nix-like storage.
data StorePath = StorePath
  { name :: String
  , hash :: Hash
  }
  deriving (Show)

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


-- | Data for calling external process
data ProcessData = ProcessData
  { stdin   :: !(Maybe BL.ByteString) -- ^ Data to pass stdin
  , env     :: [(String,String)]      -- ^ Data for putting into environment
  , args    :: [String]               -- ^ Arguments for a process
  , workdir :: !(Maybe FilePath)      -- ^ Working directory for subprocess
  }

toTypedProcess :: FilePath -> ProcessData -> IO (ProcessConfig () () ())
toTypedProcess exe process = do
  env <- case process.env of
    [] -> pure []
    es -> do env <- getEnvironment
             pure $ env ++ es
  pure $ case process.workdir of
           Nothing -> id
           Just p  -> setWorkingDir p
       $ case process.stdin of
           Nothing -> id
           Just bs -> setStdin (byteStringInput bs)
       $ case env of
           [] -> id
           _  -> setEnv env
       $ proc exe process.args
