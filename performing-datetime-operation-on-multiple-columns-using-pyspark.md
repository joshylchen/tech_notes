# Performing DateTime operation on multiple columns using Pyspark

The datetime oeprations in pyspark are very common data manipulations.

We will look into the depth of these operations to prepare the data that can fullfill our needs.

```
from pyspark.sql.functions import col,expr,to_date,date_add,date_format,unix_timestamp,lit,to_timestamp,concat
from datetime import datetime, timedelta

```

```
data = spark.createDataFrame(
  [
    (1,5, "2022-01-01"),
    (1,6, "2022-01-08"),
    (2,7, "2022-03-09"),
    (3,5, "2022-04-10"),
    (2,3, "2016-05-21"),
    (1,7, "2016-06-1"),
    (5,3, "2016-03-21")
  ],
  ["day_offset","mins_offset", "date"]
  )
```

<figure><img src="../.gitbook/assets/1 dataframe.JPG" alt=""><figcaption><p>sample dataframe</p></figcaption></figure>

## Add constant to date column

If we want to add a constant day_offset, let's say 1 day._ \
_We can just use add\__date function.

```python
data_offset = data.withColumn("date_offset_one_day",date_add(col("date"),1))
```

<figure><img src="../.gitbook/assets/1.1 add constant.JPG" alt=""><figcaption></figcaption></figure>

As you can see, although the date column is string type, date\__add function is capable of recognizing its format and converting the output to date type automatically. There are also other PySpark SQL functions like add\__months we can use.

There is a more flexible way to add constant time to the column, which is not limited to months or date. This is to use expr function.

```python
data_offset = data.withColumn("date_offset_12_day",col("date")+ expr("INTERVAL 12 DAYS"))
```

Using expr, we can add any interval by following "INTERVAL NO. DAYS/HOURS/MINUTES" format.\
We can even to 12 days and 3 hours by using

```python
data_offset = data.withColumn("date_offset_12_day",col("date")+ expr("INTERVAL 12 DAYS 3 HOURS"))
```

<figure><img src="../.gitbook/assets/1.2 expr time.JPG" alt=""><figcaption><p>add 12 days and 3 hours by using expr</p></figcaption></figure>

## Add date offset to the date column

{% hint style="info" %}
As the data type of day\__offset and mins\_offset are both "long", we need to do the typecasting first to make the rest of the experiments easier._
{% endhint %}

<figure><img src="../.gitbook/assets/2 type casting.JPG" alt=""><figcaption><p>data preparation</p></figcaption></figure>

The First example is to create a column that shows date= certain date (for instance: 1960-01-01) + day offset.

```
data_offset = data.withColumn("zero",lit(datetime.strptime('1960-01-01', '%Y-%m-%d'))).withColumn("1960_offset",expr(f"date_add(zero,day_offset)")).drop("zero")
```

The command is relatively straightforward. We need to utilize a reference column called "zero", then we use the date\_add sprk sql.function we imported earlier to add this column to the zero column.\
In the end, we drop this reference column.

<figure><img src="../.gitbook/assets/3 date add reference.JPG" alt=""><figcaption><p>1960<em>offset column is the output column based on 1960-01-01 plus day</em>offset</p></figcaption></figure>

You may recall the previous date\__add function and ask why we can't use the date\_add function?_

_Let's have a look what parameter is required for this function_



<figure><img src="../.gitbook/assets/4 date_add function cannot take column as days.JPG" alt=""><figcaption><p>date operation failed</p></figcaption></figure>

{% hint style="info" %}
Date\_add can only take a constant integer variable as the day offset
{% endhint %}



## Add time\_offset column and time\_offset to date column

Now let's look at the final and also the most complicated case: add a time offset column, a mins_offset column to a date column._\
_So in our example, since the date column is only using the date string, it doesn't have time offset._\
_Our trading time starts at 08:00._ \
_So before we add the minsoffset, we want to the introduce the 08:00,_\
_Furthermore, we want the minsoffset multiple with 5._ \
_If it is 5 in the mins\__offset, we want to offset 5\*5=25 minutes.\
&#x20;

```python
data = data.withColumn("trading_time",lit("08:00:00"))
.withColumn("trading_datetime",concat(col("date").cast("string"),lit(" "),col("trading_time")))
.withColumn('DateTime', date_format((unix_timestamp('trading_datetime', 'yyyy-MM-d HH:mm:ss') +(col('mins_offset'))*5*60).cast('timestamp'), 'yyyy-MM-dd HH:mm:ss'))
.drop("trading_datetime")
```

We do a few things here:

1. create a trading\_time column using lit function.
2. concatenate the date string in the date column with trading\_time offset
3. A few small steps there. We convert the trading_datetime to unixtime._\
   _We are passing the string datetime format to this time conversion as well._ \
   _Tell the function, the format follows yyyy-MM-dd HH:mm:ss._\
   _After that, we add the mins\_offset. Remember to multiple 60 because we want the minutes offset not second. We need to convert this column to timestamp format as well._\
   _After this, we pass the right format to date\_format function so the result with right format_

<figure><img src="../.gitbook/assets/5 final result.JPG" alt=""><figcaption><p>Final Result</p></figcaption></figure>

{% hint style="info" %}
We may find that the unit\_timestamp time format I used was yyyy-MM-d HH:mm:ss instead of yyyy-MM-dd HH:mm:ss.\
That is because if use latter, it will bring ambiguity which is DatetimePrasing error.\
Spark cannot identify whether the date format is 2016-06-10 8:00:00 or 2016-01-1 08:00:00.&#x20;
{% endhint %}

<figure><img src="../.gitbook/assets/6 error.JPG" alt=""><figcaption></figcaption></figure>

##
