-- |
-- Standard workflows
module OKA.Flow.Std
  ( -- * Saved metadata
    SavedMeta(..)
  , stdSaveMeta
  ) where

import Control.Monad.IO.Class
import Control.Monad.State.Strict
import System.FilePath            ((</>))
import System.Directory           (createFileLink)

import OKA.Flow.Tools
import OKA.Flow
import OKA.Metadata


-- | This data type represents metadata saved as output of a flow.
--   Output is stored in @saved.json@ file and uses standard metadata
--   encoding. @saved.json@ instead of @meta.json@ (which is written
--   as well) is used in order to allow other flows to generate
--   compatible outputs.
data SavedMeta a = SavedMeta a

instance (IsMeta a) => FlowArgument (SavedMeta a) where
  parserFlowArguments = do
    path <- parseSingleArgument
    liftIO $ SavedMeta <$> readMetadata (path </> "saved.json")

-- | Save metadata value so it could be passed as parameter.
stdSaveMeta :: (IsMeta a) => a -> Flow eff (Result (SavedMeta a))
stdSaveMeta a = scopeMeta $ do
  put $ toMetadata a
  liftWorkflow () (Workflow Action
    { name = "std.SavedMeta"
    , run  = \_ _meta _args out -> do
        case _args of [] -> pure ()
                      _  -> error "stdSaveMeta does not take any arguments"
        createFileLink "meta.json" (out </> "saved.json")
    }) ()

