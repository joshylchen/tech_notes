---
cover: ../.gitbook/assets/Capture.JPG
coverY: 0
---

# Replicate SAS\_merge using pyspark

## Pyspark code breakdown

Following is the Pyspark code breakdown of mimicing SAS merge function

![python function steps](../.gitbook/assets/pyspark\_merge.JPG)

{% hint style="info" %}
The for loop function has been avoided as much as possible to allow better performance.
{% endhint %}

### Import libraries

```python
from pyspark.sql.functions import col,coalesce,row_number,desc, greatest as _greatest, monotonically_increasing_id,concat
from pyspark.sql.window import Window
```

The merge function takes left-table(table A) as df1, right-table (table B) as df2, join type as "left/right/inner/full" and join key as the unique identifier(s) in the list.

```python
def SAS_merge(df1,df2,join_type,join_key):
```

### Sorting

Since the tables contain one-to-many or many-to-many relationships, there are duplicated unique identifiers(key). The sorting order will greatly affect the result of the merge (reflects in the uncommon column replication step).

{% hint style="warning" %}
Tables need to be sorted before passing to the function as SAS BY statement.
{% endhint %}

### Error and Exception

```python
if not isinstance(join_key, list):
    raise TypeError("join_key must be a list")
tablea = df1 
tableb = df2
if join_type not in ["left","right","inner","full"]:
    raise ValueError("please use one of left, right, inner, full as join type")


```

There are unlist and list handling processes in the following steps. \
The join\_key has to be passed in a list to avoid the error

The join\_type should be part of the aforementioned join types because the first step SQL join will produce different results based on the type.



```python
tablea = tablea.withColumn(
    "idx",
    row_number().over(Window.orderBy(monotonically_increasing_id()))+10)
tableb = tableb.withColumn(
    "idx",
    row_number().over(Window.orderBy(monotonically_increasing_id())))
```

Due to the one-to-many and many-to-many scenarios, key is not unique. The two types of row indexing are added to provide record uniquness.

{% hint style="info" %}
The +10 offset in tablea is not necessary. The offset here provides the transparency of the original location of the data before relocating for troubleshooting, if needed.
{% endhint %}

The "idx" field is the table-based indexing. It indicates the location of each record in associated tables.\


### Step 1: SQL Join

```python
df_left_exclude = tablea.join(tableb,join_key,"left_anti")
df_right_exclude = tableb.join(tablea,join_key,"left_anti")
df_left_exclusion_list = df_left_exclude.withColumn("key_concat",concat(*join_key)).select("key_concat").rdd.flatMap(lambda x: x).collect()
df_right_exclusion_list = df_right_exclude.withColumn("key_concat",concat(*join_key)).select("key_concat").rdd.flatMap(lambda x: x).collect()
df_exclusion = df_left_exclusion_list.copy() + df_right_exclusion_list.copy()
if join_type == "left": 
    df_exclude = df_left_exclude.drop("idx")
if join_type == "right":
    df_exclude = df_right_exclude.drop("idx")
if join_type == "inner":
    df_exclude = None
if join_type == "full":
    df_exclude = df_left_exclude.unionByName(df_right_exclude,allowMissingColumns=True).drop("idx")
```

df\_left\_exclude and df\_right\_exclude are two dataframes that contain the uncommon keys with either left or right join. This follows with the SQL join logic thus spark join function has been used.\
The column "idx" is dropped before the final merge.

The df\_exclusion is the list contain all the uncommon keys.

### Step2 : Find max records and their associated table from the common key

```python
tablea_groupby = tablea.groupby(join_key).count().withColumnRenamed("count","tablea_count").withColumn("key_concat",concat(*join_key))
tableb_groupby = tableb.groupby(join_key).count().withColumnRenamed("count","tableb_count").withColumn("key_concat",concat(*join_key))
#take out the unique keys
tablea_groupby = tablea_groupby.filter(~col("key_concat").isin(set(df_exclusion))).drop("key_concat")
tableb_groupby = tableb_groupby.filter(~col("key_concat").isin(set(df_exclusion))).drop("key_concat")
#find the max count
df_table = tablea_groupby.join(tableb_groupby,join_key,"left").withColumn("max_count",_greatest(col("tablea_count"),col("tableb_count")))
```

The left and right table groupby count produce the number records for each uncommon key.\
After the uncommon keys are removed from the filter function, the rest of the keys are either contain one-to-many or many-to-many relationships, thus they appear in both of the two tables.\
\_greatest function does the row-level comparison and provides the great count from either tables.\


```python
tablea_columns = tablea.columns
tableb_columns = tableb.columns
common_columns = list(set(tablea_columns).intersection(tableb_columns))
common_columns_wokey = common_columns.copy()
common_columns_wokey = [x for x in common_columns_wokey if x not in join_key]
uncommon_columns = list(set(tablea_columns).symmetric_difference(tableb_columns))
uncommon_tablea_columns = list(set(tablea_columns).symmetric_difference(common_columns_wokey))
uncommon_tableb_columns = list(set(tableb_columns).symmetric_difference(common_columns_wokey))  
```

### Step3 and 4: Find max record from either table

The column names for each tables, uncommon and common column names are fetched for later steps.

```python
df_tableb = df_table.filter(df_table.max_count==df_table.tableb_count)
#filter out when max count equals both left and right
df_common_left = df_table.where(df_table.max_count!=df_table.tableb_count)
df_tablea = df_common_left.filter(df_table.max_count==df_table.tablea_count)
tablea_common = df_tablea.join(tablea.select(common_columns),join_key,"left")#use common A table index
tableb_common = df_tableb.join(tableb.select(common_columns),join_key,"left")#use common B table index
table_combined = tablea_common.unionByName(tableb_common)
table_combined = table_combined.sort([*join_key,"idx"]).select(common_columns)
```

df\__tableb and df\_tablea contain the greatest records of either table._ \
_A special case is considered here when max count equals both tableA and tableB._

df\__tablea join with tablea.common\_columns preserve common columns data and max count._\
_So when max count doesn't come from tablea, these records will not appear in dftablea. In the other words, the left join will exclude them and their common\__columns selection.

After the common fields are produced, they are combined together to produce the common column results.

Here, the table\__combined total record count can match the common\_key record count after SAS merge._

### Table-wise row-based index

```python

#create key based row number for the unique identifier
table_combined = table_combined.withColumn("row",row_number().over(table_combined_key))
uncommon_key = uncommon_columns.copy()
uncommon_key.extend(join_key)
table_uncommon_key = Window.partitionBy(join_key).orderBy(*join_key,"idx") #drop idx later to avoid mixing the order.
tablea_uncommon_row = tablea.withColumn("row",row_number().over(table_uncommon_key)).drop("idx").select(uncommon_tablea_columns +["row"])
tableb_uncommon_row = tableb.withColumn("row",row_number().over(table_uncommon_key)).drop("idx").select(uncommon_tableb_columns + ["row"])
```

{% hint style="info" %}
There are two types of indexing in this function: left/right table based indexing and merge table based indexing
{% endhint %}

The merge table based indexing is called "row".

### Step5: Uncommon column filling and replicating

```python
table_concat = table_combined.join(tablea_uncommon_row,[*join_key,"row"],"left").sort(*join_key,"idx")
table_concat_2 = table_concat.join(tableb_uncommon_row,[*join_key,"row"],"left").sort(*join_key,"idx")
tablea_uncom_gap = table_combined.join(tablea_uncommon_row,[*join_key,"row"],"left_anti").sort(*join_key,"idx")
tableb_uncom_gap = table_combined.join(tableb_uncommon_row,[*join_key,"row"],"left_anti").sort(*join_key,"idx")
```

By doing the left join with uncommon row table with combined table, the roginal common\_column table is enriched with uncommon field.

```python
table_uncommon_group = Window.partitionBy(join_key).orderBy(*join_key,desc("row"))
#value to be used for back fill
tablea_uncommon_row_desc = tablea_uncommon_row.withColumn("row_2",row_number().over(table_uncommon_group)).filter(col("row_2") == 1).drop("row_2","row")
tableb_uncommon_row_desc = tableb_uncommon_row.withColumn("row_2",row_number().over(table_uncommon_group)).filter(col("row_2") == 1).drop("row_2","row")n
```

The descending sort picks the last value from its sequential order and stored in the dataframe with "row_2" indexing. This "row\_2" is only the temporary indexing to pick up the last value and dropped straight away._&#x20;

{% hint style="info" %}
_The sql.last() method was previously considered over here, which could provide greater simplicity. However, the last cannot differentiate when the last value is NULL. Thus this approach has been avoided._
{% endhint %}

```python
#add row into the uncommon column list
uncommon_tablea_columns_row = uncommon_tablea_columns.copy()
uncommon_tablea_columns_row.append("row")
uncommon_tableb_columns_row = uncommon_tableb_columns.copy()
uncommon_tableb_columns_row.append("row")
#uncommon column names without row and contract account
uncommon_tablea_columns_only = uncommon_tablea_columns.copy()
uncommon_tablea_columns_only = [x for x in uncommon_tablea_columns_only if x not in join_key]
uncommon_tableb_columns_only = uncommon_tableb_columns.copy()
uncommon_tableb_columns_only = [x for x in uncommon_tableb_columns_only if x not in join_key]
```

Create uncommon columns name list for later processes.

```python
#form a new backfill dataframe
tablea_uncommon_filled = tablea_uncom_gap.join(tablea_uncommon_row_desc,join_key,"left").select([col(x) for x in uncommon_tablea_columns_row])
tableb_uncommon_filled = tableb_uncom_gap.join(tableb_uncommon_row_desc,join_key,"left").select([col(x) for x in uncommon_tableb_columns_row])
#avoid ambiguous naming
for val in uncommon_tablea_columns_only:
    tablea_uncommon_filled = tablea_uncommon_filled.withColumnRenamed(val,(val+"_2_"))
for val in uncommon_tableb_columns_only:
    tableb_uncommon_filled = tableb_uncommon_filled.withColumnRenamed(val,(val+"_2_"))
#avoid joining with empty dataset
if tablea_uncommon_filled.rdd.isEmpty() == False:
    table_concat_2 = table_concat_2.join(tablea_uncommon_filled,[*join_key,"row"],"left").sort(*join_key,"idx")
if tableb_uncommon_filled.rdd.isEmpty() == False:
    table_concat_2 = table_concat_2.join(tableb_uncommon_filled,[*join_key,"row"],"left").sort(*join_key,"idx")
table_concat_full = table_concat_2.alias("table_concat_full")
```

The last row dataframes are joined with gap dataframe.\
The associated columns are renamed to avoid ambiguity.

A few special cases are considered over here. When the dataframe is empty, but schema is not (max records are all from left table or right table only.), the spark join will produce X number of records with column headers with "\_2" as postfix.\
The if condition here handles these two cases explicitly.&#x20;

{% hint style="info" %}
Spark join with an empty dataframe with schema will put "\_2" over all the columns.
{% endhint %}

```python
#merge two columns for backfilling
#only merge when dataset is not empty
if tablea_uncommon_filled.rdd.isEmpty() == False:
    for val in uncommon_tablea_columns_only:
        table_concat_full = table_concat_full.withColumn(val,coalesce(col(val),col(val+"_2_"))).drop(val+"_2_")

if tableb_uncommon_filled.rdd.isEmpty() == False:
    for val in uncommon_tableb_columns_only:
        table_concat_full = table_concat_full.withColumn(val,coalesce(col(val),col(val+"_2_"))).drop(val+"_2_")

table_uncommon_merge = table_concat_full.alias("table_uncommon_merge")
```

Merge two datasets with backfilling when its not empty.

### Step6: Common field overwrite

```python
common_columns_only = common_columns_wokey.copy()
common_columns_only.remove("idx")
tableb_common_override = df_tablea.join(tableb.select(common_columns),join_key,"inner").select(common_columns)
#rename to avoid ambiguous column naming
for val in common_columns_only:
    tableb_common_override = tableb_common_override.withColumnRenamed(val,(val+"_2_")).sort(*join_key,"idx")
#create row number for proper indexing
comm_override_index = Window.partitionBy(join_key).orderBy(*join_key,"idx")
tableb_common_override = tableb_common_override.withColumn("row",row_number().over(comm_override_index)).drop("idx")
#create a new override column
table_override = table_uncommon_merge.join(tableb_common_override,[*join_key,"row"],"left").sort(*join_key,"row")
#merge the result and drop
if common_columns_only == []:
    #no common_columns
    table_shared_combined = table_override.drop("row","idx")
else:
    for val in common_columns_only:      
        table_override = table_override.withColumn(val,coalesce(col(val+"_2_"),col(val))).drop(val+"_2_")
    table_shared_combined = table_override.drop("row","idx")

if df_exclude == None:
    #inner join, don't need to stack
    return  table_shared_combined
else:
    df_final = table_shared_combined.unionByName(df_exclude, allowMissingColumns=True)
    return df_final

```

After the uncommon fields filling, the final step is the common fields overwriting. The common field overwrite has been put into the last step is because it doesn't interfere with previous steps. Furthermore, the common field dataframe preserves its index from the beginning.
