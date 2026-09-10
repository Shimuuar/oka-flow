{-# LANGUAGE ApplicativeDo #-}
-- |
module Main (main) where

import Control.Monad
import Control.Lens
import Control.Exception
import Control.Monad.IO.Class
import Control.Monad.Trans.Writer.CPS
import Data.Aeson           qualified as JSON
import Data.ByteString.Lazy qualified as BL
import Data.Traversable
import Data.Maybe       (mapMaybe)
import Data.Monoid      (Ap(..))
import Data.Foldable
import Data.Map.Strict  qualified as Map
import Data.Map.Strict  (Map)
import Data.Set         qualified as Set
import Options.Applicative
import System.Directory
import System.FilePath
import System.IO

import OKA.Flow.Core.Types
import OKA.Flow.Core.S

main :: IO ()
main
  = join
  $ customExecParser (prefs showHelpOnError)
  $ (helper <*> parser)
    `info`
    (  fullDesc
    <> header   "Program for working with oka-flow store"
    <> progDesc
       "Manipulations and checking of oka-flow store"
    )


parser :: Parser (IO ())
parser = do
  store <- strOption ( long    "store"
                    <> short   's'
                    <> metavar "DIR"
                    <> help    "path to store (default .)"
                    <> value   ".")
  act <- subparser $ mconcat
    [ cmd "fsck"    (pure cmdFsck) "Perform consistency check for a store"
    -- , cmd "closure" cmdClosure     "Compute closure of store path"
    -- , cmd "revdep"  cmdRevdeps     "Compute all dependencies of a path"
    -- , cmd "prune"   cmdPrune       "Remove all paths with given prefix and all paths that depend on them"
    ]
  pure $ act store
  where
    cmd name fun hlp = command name ((helper <*> fun) `info` header hlp)


----------------------------------------------------------------
-- Command implementations
----------------------------------------------------------------

-- | Check store
cmdFsck :: FilePath -> IO ()
cmdFsck store = do
  -- Read all store paths
  (paths,err) <- runWriterT $ readStoreEntries store
  paths_dep   <- Map.traverseWithKey (\p _ -> readDependency store p) paths
  -- Print found errors
  traverseOf_  each          (putStrLn . pprError) err
  traverseOf_ (each . _Left) (putStrLn . pprError) paths_dep
  -- Check that dependency exists
  let depExists p = case p `Map.lookup` paths_dep of
        Nothing -> Just $ "Missing dependency: " ++ storePath p
        Just _  -> Nothing
  forM_ (Map.toList paths_dep) $ \case
    (_, Left _)     -> pure ()
    (p, Right deps) -> case mapMaybe depExists $ toList deps of
      []  -> pure ()
      bad -> do
        putStrLn $ "Bad dependencies for " ++ storePath p
        for_ bad $ \e -> putStrLn ("  - " ++ e)
  return ()

-- -- | Compute closure (all dependencies)
-- cmdClosure :: Parser (FilePath -> IO ())
-- cmdClosure = do
--   path <- argument (maybeReader pathToPath) (help "Store path" <> metavar "DIR")
--   pure $ \store -> do
--     (paths0, err) <- runWriterT $ readStoreEntries store
--     paths1 <- fmap sequence $ Map.traverseWithKey (\p _ -> readDependency store p) paths0
--     for_ err (hPutStrLn stderr . pprError)
--     case paths1 of
--       Left  e     -> error $ show e
--       Right paths -> forM_ (computeClosure paths path) (putStrLn . pprPath)

-- cmdRevdeps :: Parser (FilePath -> IO ())
-- cmdRevdeps = do
--   path <- argument (maybeReader pathToPath) (help "Store path" <> metavar "DIR")
--   pure $ \store -> do
--     (paths0, err) <- runWriterT $ readStoreEntries store
--     paths1 <- fmap sequence $ Map.traverseWithKey (\p _ -> readDependency store p) paths0
--     for_ err (hPutStrLn stderr . pprError)
--     case paths1 of
--       Left  e     -> error $ show e
--       Right paths -> forM_ (computeClosure (invertMap paths) path) (putStrLn . pprPath)

-- cmdPrune :: Parser (FilePath -> IO ())
-- cmdPrune = do
--   name <- strArgument (metavar "DIR" <> help "Path prefix to remove")
--   yes  <- switch (short 'y' <> help "Really delete store paths")
--   pure $ \store -> do
--     when ('/' `elem` name) $ error "Only single path is OK"
--     -- Load paths
--     (paths0,_) <- runWriterT $ readStoreEntries store
--     paths1     <- fmap sequence $ Map.traverseWithKey (\p _ -> readDependency store p) paths0
--     pathset <- case paths1 of
--       Left  e -> error $ show e
--       Right p -> pure p
--     -- Compute reverse closure
--     hashes <- listDirectory (store </> name) 
--     let paths   = Path name <$> hashes
--         revdeps = foldl' (addToClosure (invertMap pathset)) mempty paths
--     --
--     let prefix = case yes of
--           True  -> "Deleting: "
--           False -> "Dry-run: "
--     forM_ revdeps $ \p@(Path nm h) -> do
--       putStrLn $ prefix ++ pprPath p
--       when yes $ do
--         removeDirectoryRecursive (store </> nm </> h)
        


-- computeClosure :: (Ord a) => Map a [a] -> a -> Set.Set a
-- computeClosure set = addToClosure set mempty 

-- addToClosure :: (Ord a) => Map a [a] -> Set.Set a -> a -> Set.Set a
-- addToClosure set = go where
--   go acc a
--     | a `Set.member` acc = acc
--     | otherwise          = case a `Map.lookup` set of
--         Nothing -> acc'
--         Just as -> foldl' go acc' as
--     where acc' = Set.insert a acc

-- invertMap :: Ord a => Map a [a] -> Map a [a]
-- invertMap m = Map.fromListWith (<>) [ (b,[a]) | (a,bs) <- Map.toList m
--                                               , b      <- bs
--                                               ]
  



----------------------------------------------------------------
-- Implementation
----------------------------------------------------------------

-- | Errors in store 
data Error
  = FileInStore FilePath  -- ^ Store contains file where directory is expected
  | MissingDeps StorePath -- ^ No list of dependencies
  | MissingMeta StorePath -- ^ No Metadata
  | BadDeps     StorePath -- ^ Cannot parse dependencies
  | BadMeta     StorePath -- ^ Cannot parse metadata
  | BadHash     FilePath FilePath
  deriving Show

pprError :: Error -> String
pprError = \case
  FileInStore path -> "File where directory expected: " ++ path
  MissingDeps path -> "Missing deps.json in:     " <> storePath path
  MissingMeta path -> "Missing meta.json in:     " <> storePath path
  BadDeps     path -> "Cannot parse deps.txt in: " <> storePath path
  BadMeta     path -> "Cannot parse meta.json in:" <> storePath path
  BadHash     p h  -> "Cannot parse hash in:     " <> (p </> h)



-- | Get list of all entries in a store
readStoreEntries :: FilePath -> WriterT [Error] IO (Map StorePath ())
readStoreEntries store = do
  flows <- listOnlyDirs ""
  flip foldMapM flows $ \flow -> do
    hashes <- listOnlyDirs flow
    flip foldMapM hashes $ \hash_b16 -> do
      case hashFromString hash_b16 of
        Just hash -> pure $ Map.singleton (StorePath flow hash) ()
        Nothing   -> mempty <$ tell [BadHash flow hash_b16]
  where
    listOnlyDirs path
      = filterM check =<< liftIO (listDirectory root )
      where
        root      = store</>path
        check dir = liftIO (doesDirectoryExist (root</>dir)) >>= \case
          False -> False <$ tell [FileInStore dir]
          True  -> pure True

-- | Read list of dependencies 
readDependency :: FilePath -> StorePath -> IO (Either Error (S StorePath))
readDependency store path = do
  try (BL.readFile (store </> storePath path </> "deps.json")) >>= \case
    Left  (_ :: IOException) -> pure $ Left $ MissingDeps path
    Right deps -> case JSON.eitherDecode deps of
      Left  _ -> pure $ Left $ BadDeps path
      Right x -> pure $ Right x




foldMapM :: (Foldable t, Monoid m, Applicative f) => (a -> f m) -> t a -> f m
foldMapM f = getAp . foldMap (Ap . f)
