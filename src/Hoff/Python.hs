{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
module Hoff.Python where

import           Control.Lens ((%~), (.~))
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
    y -> Left $ "Error parsing HoffSerialize.py's CBOR: " <> toS (show $ fmap (\(n,_) -> n) y)

singleTableKey :: Text
singleTableKey = "single table output" 

singleTableSourceMod :: ConvertText a Text => a -> Text
singleTableSourceMod x = toS x <> "\noutputDfs = [['" <> singleTableKey <> "', outputDf]]\n"
{-# INLINABLE singleTableSourceMod #-}

data ColumnNameConvert = OriginalColNames | CamelCaseColNames
  deriving Show
  
data PythonProcess input = PythonProcess
  { pSource       :: Text
  , pArgs         :: [String]
  , pInput        :: input
  , pEnv          :: Maybe [(String, String)]
  , pStdout       :: Maybe (Handle -> IO ())        
                     -- ^ stdout handler that will be forked/killed with when the process starts/terminates 
  , pStderr       :: Maybe (Handle -> IO ())        
  , pColumnNames  :: ColumnNameConvert
  -- ^ this is ignored in direct calls to pythonEvalByteString 
  }

type RawPythonProcess = PythonProcess (Maybe ByteString) 

basicPp :: Text -> input -> PythonProcess input
basicPp source input = PythonProcess source [] input Nothing Nothing Nothing CamelCaseColNames

makeLensesWithSuffix ''PythonProcess

originalColNames :: PythonProcess input -> PythonProcess input
originalColNames = pColumnNames_ .~ OriginalColNames


-- | this uses the given Python binary to execute a script.
--
-- The script can expect the following global variables to be defined:
--
-- 1) `jsonArg`: will contain the `pInput` value (parsed from its JSON representation)
--
-- 2) Either `inputDf` or `inputDfs`: the supplied table(s) converted to a dataframes
--
-- 
-- The script is expected to store its result dataframes in the python variable:
--
-- 3) 'outputDfs` as a list of pairs `(name, dataframe)`.
--
-- This function returns the raw bytestring that will be feed to the sdtin of the process, if any
pythonEvalHoffs :: (ToJSON j, HasCallStack) => FilePath -> PythonProcess j
  -> Either EoTable [(Text, EoTable)] -> (Maybe ByteString, IO (Either Text [(Text, Table)]))
pythonEvalHoffs pBinary pp = fmap2 cborToTables . pythonEvalHoffCbor True pBinary pp
{-# INLINABLE pythonEvalHoffs #-}

-- | same as `pythonEvalHoffs` for a single result table stored in `outputDf`
pythonEvalHoff :: (ToJSON j, HasCallStack) => FilePath -> PythonProcess j
  -> Either EoTable [(Text,EoTable)] -> (Maybe ByteString, IO (Either Text Table))
pythonEvalHoff pBinary pp = fmap2 cborToTable
  . pythonEvalHoffCbor True pBinary (pp & pSource_ %~ singleTableSourceMod)
{-# INLINABLE pythonEvalHoff #-}

-- | common helper function
pythonEvalHoffCbor :: (ToJSON j, HasCallStack) =>
  Bool -> FilePath -> PythonProcess j -> Either EoTable [(Text,EoTable)] -> (Maybe ByteString, IO ByteString)
pythonEvalHoffCbor handleOutputDfs pBinary pp inputTables = (inputBs,) $ do
  modulePath <- Hoff.Serialise.pythonModule
  pythonEvalByteString pBinary $ pp
    { pInput = inputBs
    , pSource =T.unlines $ [importHoff modulePath, inputCode, pSource pp]
               <>  bool [] [columnRenameCode pp, outputDfsScript] handleOutputDfs } 
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
        (inputCode, inputBs) = second (toS <$>) $
          case (inputTables, toS $ A.encode $ pInput pp :: ByteString) of
            (Left t, jargBs)  -> (cbor "inputDf = HoffSerialise.fromCborLoadedColumns(inputCbor[0])"
                                 ,Just $ serialise (t, jargBs))
            (Right [], "[]")  -> ("inputDfs=jsonArg={}", Nothing)
            (Right ts, jargBs)-> (cbor "inputDfs = HoffSerialise.fromCborLoadedColumnsDict(inputCbor[0])"
                                 ,Just $ serialise (ts, jargBs))
        outputDfsScript = [qc|
with open(pythonEvalByteStringOutputFile, 'wb') as f:
  HoffSerialise.toHoffCbor(outputDfs, f)

|]
{-# INLINABLE pythonEvalHoffCbor #-}

-- | this uses the given Python binary to execute a script which is expected to write its result to
-- 'a file descriptor, whose path will be stored in variable `pythonEvalByteStringOutputFile`
-- 
-- `sys.argv[1:]` will contain the args passed to this function
pythonEvalByteString :: HasCallStack =>  FilePath -> RawPythonProcess -> IO ByteString
pythonEvalByteString pBinary PythonProcess{..} = do
  (outputRead, outputWrite) <- createPipe

  scriptPath <- writeSystemTempFile "hoff_python.py" $ initArgsSource <> toS pSource 

  let maybePipe a = bool a CreatePipe . isJust
      procDescr =
        (proc pBinary $ [scriptPath, fdPath outputWrite] <> pArgs)
        { std_in = maybePipe NoStream pInput
        , std_out = maybePipe Inherit pStdout , std_err = maybePipe Inherit pStderr
        , env = pEnv }

  withCreateProcess procDescr $ \stdinH stdoutH stderrH procH -> do

    -- this line is crucial or the outputRead handle would never be closed, and this
    -- whole block never return
    forkIO $ waitForProcess procH >> closeFd outputWrite

    let handler h = mapM $ forkIO . race_ (waitForProcess procH) . expectH h
    handler stdoutH pStdout
    handler stderrH pStderr
  
    forM pInput $ \input -> seq input $ expectH stdinH $ \h -> forkIO $ do
      S.hSetBinaryMode h True -- not sure if this is needed
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

        

expectH :: Maybe Handle -> (Handle -> c) -> c
expectH = forMaybe (Prelude.error "did you set std_* = CreatePipe?")


handleLinesWith :: (ByteString -> IO ()) -> Handle -> IO ()
handleLinesWith f h = catchIOError (forever $ B.hGetLine h >>= f)
                      $ \e -> if isEOFError e then pass else ioError e

-- | python code will raise an exception if renaming introduces duplicate columns
columnRenameCode :: PythonProcess input -> Text
columnRenameCode PythonProcess{..} = case pColumnNames of
  OriginalColNames -> ""
  CamelCaseColNames -> camelCaseOutputDfColumns
  where camelCaseOutputDfColumns = [q|

import stringcase
import re

for _,outputDf in outputDfs:

  renamed = [stringcase.camelcase(re.sub('[^0-9a-z]+',"_", x.lower())) for x in outputDf]
  
  if 0:
    print("renaming columns from:\n", list(outputDf))
    print("to:\n", renamed)
  
  if len(set(renamed)) != len(set(outputDf.columns)):
    raise ValueError('Renamed column names are not unique:\nOriginal:\n' + \
      str(list(outputDf.columns)) + "\nRenamed:\n" + str(renamed))

  else:
    outputDf.columns = renamed

|] :: Text
