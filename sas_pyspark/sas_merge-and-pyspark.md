---
cover: ../.gitbook/assets/merge.jpg
coverY: 0
---

# SAS\_Merge and Pyspark

## Objective

SAS merge is a special type of "join" that combines the tabular data together.  When handling the big dataset, the customized spark function was designed to mimic this merging behaviour.

## One-to-One and One-to-None relationships

In terms of _one-to-one_ or _one-to-none_ relationship, SAS merge operates the same as SQL join.\
For instance, \


```
data TableC;
merge TablA (in=a) TableB (in=b);
by id;
run;
```

{% hint style="info" %}
**Notes**: SAS is not case-sensitive.
{% endhint %}

When a=1, b=0, it is equivalent to SQL left join. a=0,b=1 equals to SQL right join and a=1, b=1 matches SQL inner join.

However, when the relationship is one-to-many or many-to-many, SAS merge operates totally different.\
&#x20;

## One-to-Many and Many-to-Many relationship

![](../.gitbook/assets/merge\_explain.JPG)

{% hint style="info" %}
**common key**: the unique identifier of records with a one-to-many or many-to-many relationship.
{% endhint %}

### Max records of common key

The total number of records in table C, after the merge, is defined by the number of records from SQL join(_one-to-one_ or _one-to-none_ relationship part) **plus** maxium records for each unique\_key (_one-to-many_ or _many-to-many_ relationship part).

\
SAS merge data manipulation will compare the records number from both table A and table B and take whichever table contains the most records.

\
In our example, there are two records with Account:10353540, three with Account:10420150 and three with Account: 10420888 in final table C.

### Common Column

SAS merge handels the data differently depending on whether they are common columns (same column header names in both table A and table B) or uncommon columns (different column header name exists in either table A or table B).

Regardless of merge type(left/right/inner/full), common columns have same merging rule.\
&#x20;

{% hint style="info" %}
Common column merging has no correlation with merge type.
{% endhint %}

#### Step1: fetch the data from the table with max records

SAS fetches the column field data from the table containing the most records.\
In our example, _bird_ and _elephant_ from Account: 10353540, _human_, _mice_ and _micky_ from Account: 10420150 and _rat_, _mouse_ and _sheep_ from Account: 10420888 are fetched.\


#### Step2: Right-Hand-Side table overwrite

After the aforementioned records are fetched, the data from the right table (table B) overwrites the common column data with the current sequential order. \
For instance, _human_ and _elephant_ are taken to replace the first two values, which are _rat_ and _mouse_.

### Uncommon Column

{% hint style="info" %}
Uncommon column merging has no correlation with merge type.
{% endhint %}

#### Uncommon column from the table with max records

![](../.gitbook/assets/merge\_explain2.JPG)

From the table which contains the max records of an associated unique id (Account), the number of values in the uncommon field matches the max record number count so the values are simply replicated across.

#### Uncommon column from the table without max records

![](../.gitbook/assets/merge\_explain3.JPG)

Since the table without max records contains lesser fields, the last value, according to the order, is replicated X times.\
X equals the number of max records minus the number of records in the current table.

For instance, there are three 10420150 produced in table C. Since there are only two records in table A Uncom\_1 field, the last value e will be copied 3-2=1 time and fill the gap

$$
X = max(tableA.id,tableB.id) - min(tableA.id,tableB.id)
$$



### ​Union

![](../.gitbook/assets/merge\_final.JPG)

The last step is to combine all three scenarios together. Records 1 and 2 are from SQL type of joining and the rest of the records are from special SAS merge join.
