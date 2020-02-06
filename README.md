# red-pandas
Lightweight Python data frames without bloat or typecasting, using only the standard library. Great for Lambdas

## Project inspiration

Pandas is great for hardcore analytical workloads. However, If you are using Pandas for convenient-but-basic dataframe operations in a non-analytical use case, you might encounter the following limitations:
- Pandas file size is large - hard to use in size-constrained places e.g. Lambda functions
- NumPy file size is large - ditto above
- Pandas transforms numbers to numpy types and dates to pandas.Timestamp - this leads to unpredictable results
- Pandas has a bloated API with several ways to accomplish a goal
- Pandas sometimes returns some subset of a dataframe with a link to the original, instead of making a new dataframe
- Pandas throws strange errors while allowing operations to work - instead of throwing clear errors that are real exceptions

It is to solve these problems that I want to build red-pandas: a smaller, simpler dataframe tool that relies only on the standard library and is generally a drop-in replacement for the Pandas API. 

## Notes

Some inspiration on organization, structure, and some copypasta from https://github.com/paleolimbot/dflite. red-pandas borrows much from @paleolimbot 's implementation of loc and iloc.

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
