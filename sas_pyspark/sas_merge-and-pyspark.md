---
cover: ../.gitbook/assets/New1-Merging-SQL-vs.-Data-Step.jpg
coverY: 0
---

# SAS\_Merge and Pyspark

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
Notes: SAS is not case-sensitive.
{% endhint %}

When a=1, b=0, it is equivalent to SQL left join. a=0,b=1 equals to SQL right join and a=1, b=1 matches SQL inner join.

However, when the relationship is one-to-many or many-to-many, SAS merge operates totally different.\
&#x20;

## Our Values

{% hint style="info" %}
**Good to know:** company values are statements about how you approach work; how you treat colleagues, customers and users; and what your company stands for.
{% endhint %}

### Be Compassionate

We treat everyone we encounter with compassion, seeing the humanity behind their problems and experiences.

### Be Mindful

We do not take advantage of our users' attention and adopt mindful working practices so that we can create safe spaces both in our working environment and in our products themselves.

### Research First

We challenge our own and others' assumptions through qualitative and quantitative research. Not sure about an idea? Test it.
