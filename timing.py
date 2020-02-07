import pandas as pd
import red_pandas as rp
import time

def t(x,s):
    print( str(round((time.time()-x)*1000,15)/1000).ljust(20) , s)

for x in (pd,rp):
    print(x.__name__)

    s = time.time()
    df = x.read_csv('red_pandas/MOCK_DATA_15k.csv') 
    t(s,'read data')

    s = time.time()
    temp = df.loc[150,'last_name']
    t(s, 'loc single value')

    s = time.time()
    temp = df.loc[999,'last_name']
    t(s, 'loc single row')

    s = time.time()
    temp = df.loc[:,'last_name']
    t(s, 'loc single col')

    s = time.time()
    temp = df[['first_name', 'last_name', 'email', 'gender']]
    t(s, 'loc mulitiple col')

    s = time.time()
    temp = df.loc[150:900]
    t(s, 'loc rows')

    s = time.time()
    temp = df.loc[100:740, ['first_name', 'last_name', 'email', 'gender'] ]
    t(s,'loc rows and columns')

    s = time.time()
    temp = df.iloc[961]['first_name']
    t(s,'iloc value')
    
    s = time.time()
    temp = df.iloc[544]
    t(s,'iloc row')

    s = time.time()
    temp = df.iloc[250:999, :4]
    t(s,'iloc rows and columns')

