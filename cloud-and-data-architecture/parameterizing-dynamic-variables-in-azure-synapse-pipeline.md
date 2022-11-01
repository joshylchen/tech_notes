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

Firstly, we create a Synapse Notebook.

<figure><img src="../.gitbook/assets/2 Create Synapse Notebook.JPG" alt=""><figcaption><p>Synapse Notebook</p></figcaption></figure>

Some simple scripts to print out the cymd and cym values.&#x20;

The first cell has to be set as parameter cell in order to take the variables from pipeline.

<figure><img src="../.gitbook/assets/3 Parameter cell.JPG" alt=""><figcaption><p>Parameter Cell</p></figcaption></figure>

{% hint style="info" %}
In databricks, instead of creating parameter cell, we use _dbutils.widgets.get() to get pipeline parameters._
{% endhint %}

Now we create a new pipeline. Click on any blank area in order to create pipeline variables.

<figure><img src="../.gitbook/assets/4 create pipeline parameter.JPG" alt=""><figcaption><p>pipeline variables</p></figcaption></figure>

We call them cym\_pipeline and cymd\_pipeline variables in order to differentiate the input variables used in the notebook. Use the aforementioned scripts for default value.

Now create a new notebook and assign the notebook we created to it.

<figure><img src="../.gitbook/assets/5 Create notebook object.JPG" alt=""><figcaption><p>Create Notebook activity</p></figcaption></figure>

Assign the right notebook using dropdown menu then create two variables used in the notebook under Base parameters session.\
The name of these two Base parameters should explicitly match variables name in the notebook.\
Use @variable() expression to capture the variable from pipeline to notebook.

<figure><img src="../.gitbook/assets/6 test run.JPG" alt=""><figcaption><p>Debug</p></figcaption></figure>

Click on debug button to trigger the test run.

<figure><img src="../.gitbook/assets/7 Log (1).JPG" alt=""><figcaption><p>Log</p></figcaption></figure>

From the Apache Spark Applications, find the job we just ran and click on Logs.

As you can see, under the stdout field, the result has been correctly printed.\


## Method 2

If we don't want to check the result using log and print, there is the other way we can use.

<figure><img src="../.gitbook/assets/8 pass to the output (1).JPG" alt=""><figcaption></figcaption></figure>

```
from notebookutils import mssparkutils
massparkutiles.notebook.exit(cymd)
```

We use mssparkutils and mssparkutils.notebook.exit(cymd) to export the notebook variable.

<figure><img src="../.gitbook/assets/9 create a set variable task.JPG" alt=""><figcaption></figcaption></figure>

We create a Set variables activity then links to Notebook1 activity on success.

<figure><img src="../.gitbook/assets/10 set variable get exitValue.JPG" alt=""><figcaption></figcaption></figure>

We need to also configure the variables field under set variable in order the capture the output exported from noteboook1.

```
@activity('Notebook1').output.status.Output.result.exitValue
```

<figure><img src="../.gitbook/assets/11 Check output from set variable.JPG" alt=""><figcaption></figcaption></figure>

<figure><img src="../.gitbook/assets/12 show the variable.JPG" alt=""><figcaption></figcaption></figure>

The output from Set variable 1 activity has successfully displayed.
