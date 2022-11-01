---
description: Parameterization in Azure Pipeline
---

# Parameterizing dynamic variables in Azure Synapse Pipeline

We have a challenge of passing two dynamic variables to a series of Synapse notebooks.

One variable is called cym, which represents current year with last month, using the format of "yyyymm".\
The other variable is called cymd, which represents current year with end of month date, using the format of  "yyyymmdd".

Let's look into how to derive these two variable first.

{% hint style="info" %}
## Create yyyymm and yyyymmdd dynamic variables based on Today's date
{% endhint %}

## Get current datetime and convert it into specific time zone.

Firstly, we will use command: utcnow(), which gives us the current utc time.

Then we convert it to different timezone. In our case, it is Western Australia Standard time.

```
convertTimeZone(utcnow(),'UTC','W. Australia Standard Time')
```

{% hint style="info" %}
The format of your time zone can be found here: [Link](https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones?view=windows-11#time-zones).
{% endhint %}

After converting to Western Australia Standard time, we need to back dated to last month.\
Now we experience a challenge here:\
Under the Microsoft [Date functions table](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-expression-language-functions#date-functions), there is no addMonths function!

<figure><img src="../.gitbook/assets/1 Date Expression.JPG" alt=""><figcaption><p>No addMonth function!</p></figcaption></figure>

This cannot stop us. Let's find a alternative approach to achieve this.

If we can get the day of current datetime as an integer, then we can use subtractFromTime function (or addDays function) to subtract this (value + 1), which gives us the last day of the previous month.

Even better!

{% hint style="info" %}
The plus arithmetic operation in expression is called add().
{% endhint %}

```
@subtractFromTime(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time'),add(dayOfMonth(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time')),1),'Day')
```

So we subtract from convertTimeZone(utcnow(),'UTC','W. Australia Standard Time') with dayOfMonth(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time')) + 1 day.

and to get the cym string and cymd string is pretty easy. We just use formatdatetime function.

```
@formatDateTime(subtractFromTime(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time'),add(dayOfMonth(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time')),1),'Day'),'yyyyMMdd')
@formatDateTime(subtractFromTime(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time'),add(dayOfMonth(convertTimeZone(utcnow(),'UTC','W. Australia Standard Time')),1),'Day'),'yyyyMM')
```

## Parameterization in Synapse Pipeline

Now let's dive into how to do parameterization in Synapse pipeline and pass them to notebooks.

