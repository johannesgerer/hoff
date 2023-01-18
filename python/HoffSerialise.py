
import pandas as pd
import os
import numpy as np
import datetime
from pathlib import Path
import cbor2
import tempfile

################       Encode         ###################

# Debug
def dfToHoffCborTemp(dfs):
    tf, filename = tempfile.mkstemp("_hoff.cbor")
    os.close(tf)
    fh = filename + ".hex"
    print("Writing cbor to",filename,"and",fh)
    cb = toHoffCbor(dfs)
    Path(filename).write_bytes(cb)
    Path(filename).write_text(cb.hex())

def default_encoder(e,v):
    if isinstance(v, pd._libs.missing.NAType): return e.encode(None)
    if isinstance(v, np.int64): return e.encode(v.item())
    assert False, str(type(v)) + ": " + str(v) 

def myCborDumps(obj):
    return cbor2.dumps(obj) #, default=default_encoder)

def myCborDump(obj, f):
    return cbor2.dump(obj, f) #, default=default_encoder)

def toHoffCbor(dfs, fileLike = None):
    obj = [[name, [seriesToHoffCbor(s, df) for s in df]] for name,df in dfs]
    # print(obj)
    if fileLike is None:
        return myCborDumps(obj)
    return myCborDump(obj, fileLike)

def seriesToHoffCbor(colName, df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Wrong type", type(df))
    s = df[colName]

    # F.. You Pyhton & Pandas: 
    # pd.core.arrays.boolean.BooleanDtype == np.dtype('O')
    # Out[117]: True

    if s.dtype not in fromDtype:
        raise ValueError("Not yet implement in HoffSerialise.py: Serialisation of " + repr(s.dtype))
    (asd, nullable) = fromDtype[s.dtype]
    (colType, func) = asd(df, s) if callable(asd) else asd
    

    # NO! the following is needed, or otherwise s2.loc[na]=None leads to the warning '
    # 'a value is trying to be set on a copy of a slice from a dataframe'0
    # if len(s) == 0: return [colType,[]]
    # YES! 
    # instead: s2=s.copy(). the problem was that some column in a df returned from read_csv had ._is_view = True
    # s2.loc[na] would modify the original df, which is never a good idea anyway

    # NO! this is not enough, because an object column can always hide nan's that are not exactly 'None'
    # this is needed to handle this:
    # toHoffCbor([('a',pd.DataFrame({'a':[np.NaN,None]}, dtype='str'))]).hex()
    # if colType == 'I None':
        # return [colType,[None]*len(s)]
    # YES! a real solution was s2.loc[na]=None

    na = s.isna()

    if nullable and na.any():
        # turn anything nullable into an object and set proper None's
        # and apply the function to non-na only (e.g. to get proper
        # ints, supported by cbor).
        #
        # is this also needed to make sure we get Haskell Nothing :: Maybe Double instead of NaN :: Double?
        if s.dtype != object:
            s2 = s.astype(object)
        else:
            s2 = s.copy()
        s2.loc[na]=None
        if not func is None:
            s2.loc[~na]=func(df,s[~na])
    else:
        s2 = s if func is None else func(df, s)

    # print(s2.dtype, s2.isna().any())
    return [colName, colType,list(s2)]

def decideObject(df, x):
    notna = x[x.notna()]
    if notna.empty:
        return ('I None', None)

    colType = type(notna.iloc[0])

    wrongType = notna.map(lambda y: type(y)!=colType)

    if wrongType.any():
        y = df[x.notna()][wrongType].copy()
        y['typeError'] = y[x.name].map(lambda v: str(type(v)) + " " + str(v))
        assert not wrongType.any(), f"column {x.name} contains non-'{colType}' objects :\n{y}"

    assert colType in recognizedObjectTypes, str(df.dtypes) + f"\n\nUnknown column type for column {x.name}: {colType}"

    return recognizedObjectTypes[colType]
    

def decideDay(df, col):
    notna = col[col.notna()]
    if notna.empty:
        return ('I None', None)

    try:
        ty = 'Maybe Day' if (notna.dt.normalize() == notna).all() else 'Maybe Time'
        return (ty, lambda _, x: x.astype(int))
    except Exception as e:
        raise Exception(f"when handling column {col.name}") from e


# pandas' disgusting auto casting, means that anything can be
# anything, especially anything can be Na.
#
# even something that is non-nullable int64 NOW will simply be convertet to float64 if a Na shows up.
#
# one of the principles behind hoff is that the columns (names and types) of external data should be
# fixed and not depend on the data within these columns.
#
# signature: (("Haskell type", transform), convert to `None`s)
# 
# set the last entry to True, to fix this error:
# _cbor2.CBOREncodeTypeError: cannot serialize type <class 'pandas._libs.missing.NAType'>
fromDtype = {np.dtype('int64')          : (('Maybe Int64'        ,None                           ), False)
             ,pd.Int64Dtype()           : (('Maybe Int64'        ,lambda _,x: x.astype(int)      ), True)
             ,np.dtype('int32')         : (('Maybe Int32'        ,None                           ), False)
             ,pd.Int32Dtype()           : (('Maybe Int32'        ,lambda _,x: x.astype(int)      ), True)
             ,pd.UInt8Dtype()           : (('Maybe Word8'        ,lambda _,x: x.astype(int)      ), True)
             ,pd.UInt32Dtype()          : (('Maybe Word32'       ,lambda _,x: x.astype(int)      ), True)
             ,pd.UInt64Dtype()          : (('Maybe Word64'       ,lambda _,x: x.astype(int)      ), True)
             ,pd.BooleanDtype()         : (('Maybe Bool'         ,lambda _,x: x.astype(bool)     ), True)
            # the above is needed to not get this error in cases where there is no NA in the colum:
             # _cbor2.CBOREncodeTypeError: cannot serialize type <class 'numpy.bool_'>
             ,np.dtype('bool')          : (('Maybe Bool'         ,None                           ), False)
             ,np.dtype('float64')       : (('Maybe Double'       ,None                           ), True)
             # see Hoff.examples.example8
             # float32 has no python equivalent and
             # is turned into float by list(pd.Series) which cbor encodes to float64
             ,np.dtype('float32')       : (('Maybe Double'        ,lambda _,x: x.astype(float)  ), True)
             ,np.dtype('<M8[ns]')       : (decideDay                                            , True)
             ,np.dtype('O')             : (decideObject                                         , True)
             }

recognizedObjectTypes = {bool           : ('Maybe Bool', None)
                         ,str           : ('Maybe Text', None)
                         ,bytes         : ('Maybe ByteString', None)
                         ,datetime.date : ('Maybe Day' , lambda _, x: pd.to_datetime(x).astype(int))
                         }

fromHaskellType = {'I Int64'            : (pd.Int64Dtype()              , None)
                  ,'I Int'              : (pd.Int64Dtype()              , None)
                  ,'I Int32'            : (pd.Int32Dtype()              , None)
                  ,'I Word8'            : (pd.UInt8Dtype()              , None)
                  ,'I Word32'           : (pd.UInt32Dtype()             , None)
                  ,'I Word64'           : (pd.UInt64Dtype()             , None)
                  ,'I Word'             : (pd.UInt64Dtype()             , None)
                  ,'I Text'             : (np.dtype('O')                , None)
                  ,'I [Char]'           : (np.dtype('O')                , None)
                  ,'I Char'             : (np.dtype('O')                , None)
                  ,'I Float'            : (np.dtype('float32')          , None)
                  ,'I Double'           : (np.dtype('float64')          , None)
                  ,'I Bool'             : (np.dtype('bool')             , None)
                  ,'I Time'             : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'I Day'              : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'I None'             : (np.dtype('O')                , None)
                  ,'I ByteString'       : (np.dtype('O')                , None)
                  ,'Maybe Int64'        : (pd.Int64Dtype()              , None)
                  ,'Maybe Int'          : (pd.Int64Dtype()              , None)
                  ,'Maybe Int32'        : (pd.Int32Dtype()              , None)
                  ,'Maybe Word8'        : (pd.UInt8Dtype()              , None)
                  ,'Maybe Word32'       : (pd.UInt32Dtype()             , None)
                  ,'Maybe Word64'       : (pd.UInt64Dtype()             , None)
                  ,'Maybe Word'         : (pd.UInt64Dtype()             , None)
                  ,'Maybe Text'         : (np.dtype('O')                , None)
                  ,'Maybe [Char]'       : (np.dtype('O')                , None)
                  ,'Maybe Char'         : (np.dtype('O')                , None)
                  ,'Maybe Double'       : (np.dtype('float64')          , None)
                  ,'Maybe Float'        : (np.dtype('float32')          , None)
                   # see Hoff.examples.example8
                   ,'Maybe Bool'         : (pd.BooleanDtype()            , None) # only difference (between what?), 
                  ,'Maybe Time'         : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'Maybe Day'          : (np.dtype('<M8[ns]')          , pd.to_datetime)
                  ,'Maybe ByteString'   : (np.dtype('O')                , None)
                  ,'I Void'             : (np.dtype('O')                , None)
                   }


def example():
    a = pd.DataFrame({'IntCol': [1,10], 'BoolCol':[True,False], 'TextCol':['sd','yi'], 'DoubleCol':[1.3, None],
                      # 'd': pd.Series([1,None], dtype='Int64')
                      })
    a['TimestampCol'] = pd.to_datetime(a.IntCol)
    print(a.dtypes)
    print({a[c].dtype:c for c in a})
    print(toHoffCbor(a).hex())
    return a

def toHoffFile(df, path):
    Path(path).write_bytes(toHoffCbor(df))

################       Decode         ###################
    
def tableFromHoffCbor(arg):
    return fromCborLoadedColumns(cbor2load(arg))

def tableDictFromHoffCbor(arg):
    return {k: fromCborLoadedColumns(v) for k,v in cbor2load(arg)}

# arg should be bytes or fileLike
def cbor2load(arg):
    return cbor2.loads(arg) if type(arg) == bytes else cbor2.load(arg)

def fromCborLoadedColumnsDict(d):
    return {k: fromCborLoadedColumns(v) for k,v in d}

def fromCborLoadedColumns(loadedCols):
    cols={}
    for (colName, colType, colData) in loadedCols:
        # print(k)
        # print(v[0])
        # print(v[1])
        (dtype, func) = fromHaskellType[colType]
        s = pd.Series(data = colData, dtype=dtype)
        cols[colName] = s if func is None else func(s)

    return pd.DataFrame(cols)
