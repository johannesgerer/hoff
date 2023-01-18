{-# LANGUAGE QuasiQuotes #-}
module Hoff.Python where

import           Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Text as T
import           Foreign ( castPtr )
import           Hoff.Serialise
import           Hoff.Table
import qualified Prelude
import           System.Directory as P
import           System.Exit
import           System.FilePath
import qualified System.IO as S
import           System.IO.Temp
import           System.Posix.IO.ByteString hiding (fdWrite)
import           System.Posix.Types
import           System.Process hiding (createPipe)
import           Text.InterpolatedString.Perl6
import           Yahp

type CommandLineArgs = [String]

cborToTables :: ByteString -> Either Text [(Text, Table)]
cborToTables b = do
  x <- tableDeserialise $ toS b
  seq x $ pure x

cborToTable :: ByteString -> Either Text Table
cborToTable  b = do
  x <- tableDeserialise $ toS b
  case x of
    [(k, y)] | k == singleTableKey -> seq y $ pure y
    y -> Left $ "Impossible python results: " <> toS (show $ fmap (\(n,_) -> n) y)

singleTableKey :: Text
singleTableKey = "single table output" 

singleTableSourceMod :: ConvertText a Text => a -> Text
singleTableSourceMod x = toS x <> "\noutputDfs = [['" <> singleTableKey <> "', outputDf]]\n"
{-# INLINABLE singleTableSourceMod #-}

-- | this uses the given Python binary to execute a script which can expect the inputTable converted
-- to a dataframe stored in python variable 'inputDf' and is expected to store its result dataframes
-- in the python variable 'outputDfs` as a list of pairs `(name, dataframe)`.
pythonEvalHoffs :: (ToJSON j, HasCallStack, ConvertText t Text)
  => FilePath -> t -> CommandLineArgs -> j -> Maybe [(String, String)] -> Either Table [(Text,Table)]
  -> IO (Either Text [(Text, Table)])
pythonEvalHoffs x y z w v a = cborToTables <$> pythonEvalHoffCbor x y z w v a
{-# INLINABLE pythonEvalHoffs #-}

-- | same as `pythonEvalHoffs` for a single result table stored in `outputDf`
pythonEvalHoff :: (ToJSON j, HasCallStack, ConvertText t Text)
  => FilePath -> t -> CommandLineArgs -> j -> Maybe [(String, String)] -> Either Table [(Text,Table)]
  -> IO (Either Text Table)
pythonEvalHoff x source z w v a = cborToTable <$>
  pythonEvalHoffCbor x (singleTableSourceMod source) z w v a
{-# INLINABLE pythonEvalHoff #-}

-- | `jsonArg` will contain the parsed json value 
pythonEvalHoffCbor :: (ToJSON j, HasCallStack, ConvertText t Text) => FilePath -> t
  -> CommandLineArgs -> j -> Maybe [(String, String)] -> Either Table [(Text,Table)] -> IO ByteString
pythonEvalHoffCbor pythonBin source args jarg env inputTables = do
  modulePath <- Hoff.Serialise.pythonModule
  pythonEvalByteString pythonBin (T.unlines [importHoff modulePath, inputCode, toS source, outputDfs])
    args env inputBs
  where importHoff modulePath = [qc|
import pandas as pd
import json
sys.path.append('{takeDirectory modulePath}')
# print(sys.path)
import {takeBaseName modulePath} as HoffSerialise
# print(HoffSerialise)

|]
        cbor x = T.unlines ["inputCbor = HoffSerialise.cbor2load(sys.stdin.buffer)"
                           ,"jsonArg = json.loads(inputCbor[1])", x]
        (inputCode, inputBs) = second (toS <$>) $ case (inputTables, toS $ A.encode jarg :: ByteString) of
          (Left t, jargBs)  -> (cbor "inputDf = HoffSerialise.fromCborLoadedColumns(*inputCbor[0])"
                               ,Just $ serialiseTableAnd t jargBs)
          (Right [], "[]")  -> ("inputDfs=jsonArg=[]", Nothing)
          (Right ts, jargBs)-> (cbor "inputDfs = HoffSerialise.fromCborLoadedColumnsDict(inputCbor[0])"
                               ,Just $ serialiseTablesAnd ts jargBs)
        outputDfs = [qc|
with open(pythonEvalByteStringOutputFile, 'wb') as f:
  HoffSerialise.toHoffCbor(outputDfs, f)

|]
{-# INLINABLE pythonEvalHoffCbor #-}

-- | this uses the given Python binary to execute a script which is expected to write its result to
-- 'a file descriptor, whose path will be stored in variable `pythonEvalByteStringOutputFile`
-- 
-- `sys.argv[1:]` will contain the args passed to this function
pythonEvalByteString :: (HasCallStack, ConvertText t String)
  => FilePath -> t -> CommandLineArgs -> Maybe [(String, String)] -> Maybe ByteString -> IO ByteString
pythonEvalByteString pythonBin source args env inputM = do
  (outputRead, outputWrite) <- createPipe

  scriptPath <- writeSystemTempFile "hoff_python.py" $ initArgsSource <> toS source 

  let procDescr =
        (proc pythonBin $ [scriptPath, fdPath outputWrite] <> args)
        { std_in = bool NoStream CreatePipe $ isJust inputM, env = env }
  withCreateProcess procDescr $ \stdinH _ _ procH -> do
    forkIO $ waitForProcess procH >> closeFd outputWrite

    forM inputM $ \input -> seq input $ forMaybe (Prelude.error "did you set std_in = CreatePipe?") stdinH $
      \h -> forkIO $ do S.hSetBinaryMode h True -- not sure if this is needed
                        B.hPut h input
                        S.hClose h

    -- print "waiting for python output"
    output <- B.hGetContents =<< fdToHandle outputRead
    waitForProcess procH >>= \case
      ExitFailure code -> throwIO $ userError $ "python Exit code: " <> show code
                          <> "\n" <> prettyCallStack callStack 
      ExitSuccess -> do output <$ removeFile scriptPath
  where initArgsSource = [q|
import sys
pythonEvalByteStringOutputFile = sys.argv.pop(1)
|]
{-# INLINABLE pythonEvalByteString #-}

fdPath :: Show a => a -> String
fdPath fd = "/dev/fd/" <> show fd
{-# INLINABLE fdPath #-}
                                   
  
-- | Write a 'ByteString' to an 'Fd'.
fdWrite :: Fd -> ByteString -> IO ByteCount
fdWrite fd bs = BU.unsafeUseAsCStringLen bs
  $ \(buf,len) -> fdWriteBuf fd (castPtr buf) (fromIntegral len)

        
