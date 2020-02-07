# duffel
Lightweight Python data frames without bloat or typecasting, using only the standard library.


```
git clone https://github.com/russellromney/duffel
cd duffel


import duffel as pd

df = pd.read_csv('duffel/MOCK_DATA.csv')

df.shape
>>> (1000, 6)

df.head()
>>>
     id    first_name  last_name  email                      gender  ip_address      
 --  ----  ----        ----       ----                       ----    ----            
 0   1     Brinn       Herity     bherity0@hugedomains.com   Female  53.183.199.223  
 1   2     Wylma       Lavell     wlavell1@stumbleupon.com   Female  206.172.62.206  
 2   3     Devin       Dearan     ddearan2@businessweek.com  Female  98.251.59.108   
 3   4     Blinni      McClary    bmcclary3@pbs.org          Female  132.90.160.192  
 4   5     Byram       Weddup     bweddup4@csmonitor.com     Male    175.16.72.226   
 5   6     Jolynn      Glanvill   jglanvill5@mail.ru         Female  205.230.55.59   
duffel.DataFrame (6, 6)

df.loc[576]
>>>
      id    first_name  last_name  email                      gender  ip_address      
 --   ----  ----        ----       ----                       ----    ----            
 576  577   Cesaro      Ohrtmann   cohrtmanng0@tuttocitta.it  Male    252.141.154.52  
duffel._Row (1, 6)

df.loc[5:7, ['first_name','gender']]
>>>
     first_name  gender  
 --  ----        ----    
 5   Jolynn      Female  
 6   Moina       Female  
duffel.DataFrame (2, 2)
```


## Project inspiration

Pandas is great for hardcore analytical workloads. However, If you are using Pandas for convenient-but-basic dataframe operations in a non-analytical use case, you might encounter the following limitations:
- Pandas file size is large - hard to use in size-constrained places e.g. Lambda functions
- NumPy file size is large - ditto above
- Pandas transforms numbers to numpy types and dates to pandas.Timestamp - this leads to unpredictable results
- Pandas has a bloated API with several ways to accomplish a goal
- Pandas sometimes returns some subset of a dataframe with a link to the original, instead of making a new dataframe
- Pandas throws strange errors while allowing operations to work - instead of throwing clear errors that are real exceptions

It is to solve these problems that I want to build ~~red-pandas~~ duffel: a smaller, simpler dataframe tool that relies only on the standard library and is generally a drop-in replacement for the Pandas API. Take your data with you, without the bloat of a 400kg full-grown bear.

## Notes

Some inspiration on organization, structure, and some copypasta from https://github.com/paleolimbot/dflite. `duffel` borrows much from @paleolimbot implementation of `loc`, `iloc`, and `__repr__`

Uses the `black` code style. https://black.readthedocs.io/en/stable/the_black_code_style.html

## Project goals

Build a dataframe solution that can be easily used in AWS Lambda functions for most non-analytical dataframe operations. 

Implement a significant subset of the "minimally sufficient" Pandas API as laid out in https://medium.com/dunder-data/minimally-sufficient-pandas-a8e67f2a2428:

**Attributes**
columns
dtypes
index
shape
T
values

**Aggregation Methods**
all
any
count
describe
idxmax
idxmin
max
mean
median
min
mode
nunique
sum
std
var

**Non-Aggretaion Statistical Methods**
abs
clip
corr
cov
cummax
cummin
cumprod
cumsum
diff
nlargest
nsmallest
pct_change
prod
quantile
rank
round

**Subset Selection**
head
iloc
loc
tail

**Missing Value Handling**
dropna
fillna
interpolate
isna
notna

**Grouping**

expanding
groupby
pivot_table
resample
rolling

**Joining Data**
append
merge

**Other**
asfreq
astype
copy
drop
drop_duplicates
equals
isin
melt
plot
rename
replace
reset_index
sample
select_dtypes
shift
sort_index
sort_values
to_csv
to_json
to_sql

**Functions**
pd.concat
pd.crosstab
pd.cut
pd.qcut
pd.read_csv
pd.read_json
pd.read_sql
pd.to_datetime
pd.to_timedelta
