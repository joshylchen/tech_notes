---
cover: ../.gitbook/assets/Capture.JPG
coverY: 0
---

# Replicate SAS\_merge using pyspark

## Objective

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



![](../.gitbook/assets/merge\_explain.JPG)

