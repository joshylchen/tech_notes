# The intuition behind the Linear Discrimination Model - DevOps edition

Linear Discrimination Model (aka: Linear Discriminant Analysis/LDA) was invented by Sir Ronald Fisher in 1936. It is related to variance analysis (ANOVA) and regression. \
Although it is a simply and classical linear model, which hasn't been that popular anymore comparing with other sophiscated machine learning models, the mathematical approach behind this model, however, it is funcinating. \
Let's dive into it.

### Classification and Dimention reduction

<figure><img src="../.gitbook/assets/classification (1).jpg" alt=""><figcaption><p>What is classification?</p></figcaption></figure>

Demention reduction is generally considered as preprocessing step when constructing classfication model.\
We don't need to memorize every detail of Batman in order to tell that he is not Captain America.\
Instead, we can just recognize that the colour scheme of Captain America is red and blue, but Batman is black instead.\
When designing a machine learning model, we would also like it to capture the essential "unique identifiers" between two classes to avoid overfitting.  The generlization capability follows Occams' Razor principle.

{% hint style="info" %}
Occams' Razor: "**entities should not be multiplied beyond necessity**"
{% endhint %}

<figure><img src="../.gitbook/assets/xy.png" alt=""><figcaption><p>Red and black samples in the Cartesian coordinate system</p></figcaption></figure>

Let's look at this example. There are some red and black dots in two-dimensional coordinate systems. X and Y axis are used to recode their location in the space.\
By using Occams' Razor principle, we could possibly just use one axis to record these points, which is sufficient for us to deliver our objective (classification).

Since X and Y values are these points' projections on one axis respectively, we can use find one axis to be the new projection baseline.

<figure><img src="../.gitbook/assets/z one.png" alt=""><figcaption><p>Is this Z axis sufficient?</p></figcaption></figure>

This Z axis is not the candidate as the red and black projections are clustered together.

### App design and data integration

<figure><img src="../.gitbook/assets/Monolithic architecture.png" alt=""><figcaption><p>Monolithic Architecture</p></figcaption></figure>

Monolithic architecture is a traditional, self-contained application. It is very easy to be deployed and relatively simple to be developed. The load balancer can be deployed to increase scalability.

<figure><img src="../.gitbook/assets/monolithic load balancer.png" alt=""><figcaption><p>Monolithic with load balancer</p></figcaption></figure>

However, the drawback of monolithic architecture is significant as well.\
Considering we have 500 clients interact with our E-Commerce application,&#x20;
