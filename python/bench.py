import numpy as np
import pandas as pd
n = 1000000
df = pd.DataFrame(np.random.randint(0,2**63-1,size=(n, 1)), columns=list('a'))
df['b']=a.astype(str)
# %time df.a.sort_values()
# %time df.b.sort_values()
a = np.random.randint(0,2**63-1,size=n)
# %time np.argsort(a)

b = df.b.values
# %time np.argsort(a)
