# Efficient replication of SAS counter function using Pyspark

We have a dataset with _id, date and a in\_default column to record whether user paid their bill or not._\
_The in\_default is a binary value contains 1 as user failed to pay the bill and 0 as user paid the bill._\
_The objective is to count the number of consecutive days that user paid the bill._

_Let's look at the dataset to develop a more intuitive insight._

![sample data in SAS](../.gitbook/assets/test\_data\_1.JPG)

SAS conducts the window function counter in a very concise way.

![sas window counter function](<../.gitbook/assets/sas counter.JPG>)

```
data data_cure;
set test;
 by id date;
 if first.id then cure_month =0;
retain cure_month 0;
if in_default =0 then cure_month = sum(cure_month ,1);
if in_default = 1 then cure_month =0;
run;
```

Firstly, SAS sorts the data by id then date.\
After the sorting, it will fill the first id with 0 then accumulate the counter with 1 with every consecutive in\__default=0._ \
_Whenever there is a in\_default = 1, it will reset this counter to 0._&#x20;

### First attempt

Let's see how we can replicate this logic using Pyspark.

Loop/while counter is the first function that pop up in our mind but it is not an efficient way to use.

Firstly, we create this dataset using Pyspark.

```
simpleData1 = [("1","01/01/2022",1), \
    ("1","02/01/2022",1), \
    ("1","03/01/2022",1), \
    ("1","04/01/2022",0), \
    ("1","05/01/2022",0), \
    ("1","06/01/2022",0), \
    ("1","07/01/2022",0), \
    ("1","08/01/2022",0), \
    ("1","09/01/2022",1), \
    ("2","01/01/2022",1), \
    ("2","02/01/2022",0), \
    ("2","03/01/2022",0), \
    ("2","04/01/2022",0), \
    ("2","05/01/2022",0), \
    ("2","06/01/2022",1), \
    ("2","07/01/2022",1), \
    ("2","08/01/2022",0), \
    ("2","09/01/2022",0), \
    ("3","01/01/2022",0), \
    ("3","02/01/2022",1), \
    ("3","03/01/2022",1), \
    ("3","04/01/2022",1), \
    ("3","05/01/2022",0), \
    ("3","06/01/2022",0), \
    ("3","07/01/2022",0), \
    ("3","08/01/2022",0), \
    ("3","09/01/2022",0)\
  ]
columns1= ["id","date","in_default"]
tablea = spark.createDataFrame(data = simpleData1, schema = columns1)
tablea = tablea.withColumn("date",to_date(col("date"),"dd/MM/yyyy"))
```

We need to introduce row\_number as the window function counter.

```
from pyspark.sql.functions import col,to_date,row_number
from pyspark.sql.window import Window
```

{% hint style="info" %}
Let's see what the window and row\_number functions will bring to us.
{% endhint %}

```
in_default_window = Window.partitionBy("id").orderBy("date")
tableb = tablea.withColumn("col1",when(col("in_default")==0,(row_number().over(in_default_window))))
tableb.show()
```

![window function](<../.gitbook/assets/windows function (1).JPG>)

&#x20;The window function will do accumulated counting and ignore in\__default=1 cases._\
_However, what we want to achieve is the counter that_ can be resets when in\_default = 1.

We may got an idea of partitioning the in\__default as well, so row\__number will only count in chunks.

![idea of partitioning](<../.gitbook/assets/windows function partitioned.JPG>)

Let's see how it will behave when we add in\_default into partitioning.

```
in_default_window = Window.partitionBy("id","in_default").orderBy("id","date")
tableb = tablea.withColumn("col1",when(col("in_default")==0,(row_number().over(in_default_window)))).orderBy("id","date")
tableb.show()
```

![Add in\_default into partitioning](<../.gitbook/assets/after in\_default partitioned.JPG>)

As you can see, this is not quite like what we want to achieve previously. We want whenever in_default =1, the consecutive in\__default=0 counter will start from 1 again.

When we add in_default into partition, there are only two partitions for each id: indefault=0 partition and in\__default=1 partition.

{% hint style="info" %}
The way to achieve this is by using nested window functions.
{% endhint %}

Firstly, we create a window function to bucket out the id.\
Then, we use the window function bucketed by id and sort by the transient column. (col2)\
We also introduce dense\_rank here.&#x20;

![](<../.gitbook/assets/dense\_rank vs rank.png>)

{% hint style="info" %}
The dense\_rank will keep the current rank when the same value appears.
{% endhint %}

![Pyspark Implementation 1](../.gitbook/assets/pyspark\_implementation1.JPG)

```
in_default_window1 = Window.partitionBy("id").orderBy("date")
in_default_rank_window = Window.partitionBy("id").orderBy("col2")
replicas = last("col1", ignorenulls = True).over(in_default_window1 )
tablea_cure = tablea.withColumn("col1",when(col("in_default")==0,(row_number().over(in_default_window1)))).withColumn("col2",when(col("in_default")!=0,replicas).otherwise(col("col1"))).withColumn("count",(dense_rank().over(in_default_rank_window)-1))
tablea_cure = tablea_cure.withColumn("count_reset",when(col("in_default")==1,0).otherwise(col("count"))).sort("id","date")
```

The transient column col2 works like this: when the in_default equals to 1, it will use the last value  from col1 then forward filling consecutive rows. Otherwise, when in\__default equals to 0, it just fill follwos the col1.

![Forward filling the last value](<../.gitbook/assets/last value replication (1).JPG>)

The reason we are doing this is to leverage the beauty of dense_rank function: it will keep the current rankcounter with same value, without accumulating._\
_After this we reset when in\__default = 1 case to 0.

![issue with counter resetting](<../.gitbook/assets/Not reset.JPG>)

Everything seems right with our great effort but the consecutive in\_default=0 after 1 didn't reset! (highlighted in yellow.)



## Pyspark Implementation Solution

{% hint style="success" %}
Now it is time for the proper solution.
{% endhint %}

We still use three nested window functions, but this time, we implementing the idea of offset.&#x20;

```
tablea_window1 = Window.partitionBy("id").orderBy("date")
tablea_window2 = Window.partitionBy("id","in_default").orderBy("date")
tablea_window3 = Window.partitionBy("id","in_default","offset").orderBy("date")
tablea_cure1 = tablea.withColumn("offset",(row_number().over(tablea_window2)-(row_number().over(tablea_window1)))).withColumn("cure_month",when(col("in_default")==1,0).otherwise(row_number().over(tablea_window3))).orderBy("id","date")
```

![Final solution](<../.gitbook/assets/final solution.JPG>)

Firstly, we create offset bucket.\
This offset bucket is the row\_number count of window1 and window2.

![blue is window 1 and red is window 2](<../.gitbook/assets/final solution buckets.JPG>)

The highlighted blue is partitioned by id, so the row_number with windows 1 is a arithmetic sequence from 1 to 9. The second window partitioned on both id and in\__default, so they are two arithmetic sequences respectively. The differences of these arithmetic sequences are the artificial partition(bucket) we will use in last step.

{% hint style="info" %}
Artificial partition
{% endhint %}

![artificial partition](<../.gitbook/assets/artificial partition.JPG>)

The last step, it will utilized all three partitions, to target the same id(blue), same in_default(red) value then same offset(black) for the row\__number count.&#x20;

{% hint style="success" %}
Mission completed
{% endhint %}

