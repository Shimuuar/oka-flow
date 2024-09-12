{-# LANGUAGE UndecidableInstances #-}
-- |
-- Standard workflows
module OKA.Flow.Std
  ( -- * Saved metadata
    SavedMeta(..)
  , stdSaveMeta
  , stdJupyter
  ) where

import Control.Monad.State.Strict
import System.FilePath            ((</>))
import System.Directory           (createFileLink,createDirectory)
import System.Process.Typed
import System.IO.Temp
import System.Environment         (getEnvironment)

import OKA.Flow.Tools
import OKA.Flow.Parser
import OKA.Flow.Graph
import OKA.Flow.Types
import OKA.Metadata


-- | This data type represents metadata saved as output of a flow.
--   Output is stored in @saved.json@ file and uses standard metadata
--   encoding. @saved.json@ instead of @meta.json@ (which is written
--   as well) is used in order to allow other flows to generate
--   compatible outputs.
data SavedMeta a = SavedMeta a
  deriving FlowArgument via AsFlowOutput (SavedMeta a)

instance (IsMeta a) => FlowInput (SavedMeta a) where
  readOutput path = SavedMeta <$> readMetadata (path </> "saved.json")

-- | Save metadata value so it could be passed as parameter.
stdSaveMeta :: (IsMeta a) => a -> Flow eff (Result (SavedMeta a))
stdSaveMeta a = scopeMeta $ withoutExtMeta $ do
  put $ toMetadata a
  liftWorkflow () Action
    { name = "std.SavedMeta"
    , run  = \_ _meta args out -> do
        case args of [] -> pure ()
                     _  -> error "stdSaveMeta does not take any arguments"
        createFileLink "meta.json" (out </> "saved.json")
    } ()


-- | Run jupyter notebook. Metadata and parameters are passed in
--   environment variables.
stdJupyter
  :: (ResultSet p)
  => FilePath   -- ^ Notebook name
  -> p          -- ^ Parameters to pass to notebook.
  -> Flow eff ()
-- FIXME: We need mutex although not badly. No reason to run two
--        notebooks concurrently
stdJupyter notebook = basicLiftPhony () $ \_ meta param -> do
  withParametersInEnv meta param $ \env_param -> do
    withSystemTempDirectory "oka-flow-jupyter-" $ \tmp -> do
      let dir_config  = tmp </> "config"
          dir_data    = tmp </> "data"
      createDirectory dir_config
      createDirectory dir_data
      env <- getEnvironment
      let run = setEnv ([ ("JUPYTER_DATA_DIR",   dir_data)
                        , ("JUPYTER_CONFIG_DIR", dir_config)
                        ] ++ env_param ++ env)
              $ proc "jupyter" [ "notebook" , notebook
                               , "--browser", "chromium"
                               ]
      withProcessWait_ run $ \_ -> pure ()
