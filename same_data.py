import random
import red_pandas as rp

test_columns = 'abcdefg'
test_index = list(range(len(test_columns)))

def row_data(cols):
    return [x for x in test_columns]

def data(rows,cols):
    return [
        [f'{x}{i}' for x in row_data(cols)]
        for i,j in enumerate(range(rows))
    ]

data = data(len(test_index), len(test_columns))

dl = {
    k:v
    for k,v in zip(test_columns,[[data[j][i] for j in range(len(data[i]))] for i in range(len(data))])
}

dd = {
    i:{k:v for k,v in zip(test_columns, d)}
    for i,d in zip(test_index, data)
}

ld = [
    {k:v for k,v in zip(test_columns, d)}
    for d in data
]

ll = data

df_dd = rp.DataFrame(dd)
df_dl = rp.DataFrame(dl,index=test_index)
df_ld = rp.DataFrame(ld,columns=test_columns)
df_ll = rp.DataFrame(ll,columns=test_columns,index=test_index)
