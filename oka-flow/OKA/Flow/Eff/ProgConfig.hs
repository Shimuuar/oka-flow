{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeFamilies      #-}
-- |
module OKA.Flow.Eff.ProgConfig
  ( ProgConfigE
  , ProgConfig(..)
  , defaultProgConfig
  , runProgConfigE
  , askProgConfig
  )where

import Data.Aeson                   qualified as JSON
import Data.Aeson                   ((.:?), (.!=), (.=))
import Effectful
import Effectful.Dispatch.Static
import GHC.Generics                 (Generic)

import OKA.Flow.Core.Flow


----------------------------------------------------------------
-- Program config
----------------------------------------------------------------

-- | Workflow has access to configuration which allows to access to
--   configuration of external tool. This is intended to allow to
--   specify various programs which should not affect output and
--   mostly used in phony workflows in order to inspect results etc.
data ProgConfigE :: Effect

type    instance DispatchOf ProgConfigE = Static NoSideEffects
newtype instance StaticRep  ProgConfigE = ProgConfigE ProgConfig

-- | Configuration of a workflow
data ProgConfig = ProgConfig
  { pdf :: Maybe FilePath
    -- ^ PDF reader to use. Uses xdg-open if not specified
  , browser :: Maybe FilePath
    -- ^ Browser to use
  , jupyterNotebookDir :: Maybe FilePath
    -- ^ Directory for jupyter notebooks.
  , jupyterBrowser     :: !Bool
  }
  deriving stock (Show,Eq,Generic)

instance JSON.FromJSON ProgConfig where
  parseJSON = JSON.withObject "ProgConfig" $ \o -> do
    pdf                <- o .:? "pdf"
    browser            <- o .:? "browser"
    jupyterNotebookDir <- o .:? "jupyterNotebookDir"
    jupyterBrowser     <- o .:? "jupyterBrowser" .!= False
    pure ProgConfig{..}

instance JSON.ToJSON ProgConfig where
  toJSON ProgConfig{..} = JSON.object
    [ "pdf"                .= pdf
    , "browser"            .= browser
    , "jupyterNotebookDir" .= jupyterNotebookDir
    , "jupyterBrowser"     .= jupyterBrowser
    ]

defaultProgConfig :: ProgConfig
defaultProgConfig = ProgConfig
  { pdf                = Nothing
  , browser            = Nothing
  , jupyterNotebookDir = Nothing
  , jupyterBrowser     = False
  }

runProgConfigE :: ProgConfig -> Eff (ProgConfigE : es) a -> Eff es a
runProgConfigE cfg = evalStaticRep (ProgConfigE cfg)

askProgConfig :: ProgConfigE :> es => Flow es ProgConfig
askProgConfig = Flow $ do
  ProgConfigE cfg <- getStaticRep
  pure cfg
