# Using the Spark Aggregator class in Scala

*Tags: Spark, Scala, Aggregator, Machine Learning, Dataset, DataFrame, Count Distinct*

!!! note
    If you already understand the `Aggregator` class and simply want to view an example of how it's implemented in Scala, feel free to skip to the ["Using our Aggregators"](#using-our-aggregators) section.

## Type-safe Aggregations

### What are they?

You're in an airport waiting for your luggage at the baggage claim. The internal turmoil is rising inside of you, as most other passengers have received their luggage by now and you sure don't want to lose your clothes, your toilettries, or that bottle of deluxe craft beer you checked. Just as you start to despair, you see it: a large, bright orange suitcase that is clearly yours. Gleefully, you pick it up and a smile just begins to cross your face - until you notice a drip from the zipper. And then another. And another. While the bag itself is in the same condition as it was when you checked it, its contents are soaked. You can't help but wonder, *When did this happen? Was it the low pressure in the plane? Did it fall off of a cart? Should I have used bubble wrap?* Unfortunately, you don't really have any way of knowing. Although you should have used bubble wrap.

A similar feeling of distress occurs when a Data Scientist performs an aggregation on a Dataset in Spark, only for the contents of the output Dataset to be revealed as invalid in some way; maybe the input and output were both intact Datasets, but something about the output's content type is incorrect. The internal mechanisms of the aggregation and what went wrong can typically only be diagnosed after a significant amount of detective work. Even then, there's no guarantee that future instances of input data won't cause another issue for the aggregation.

However, just like adding more bubble wrap to prevent a bottle breaking in your suitcase, we can create custom aggregations that are strongly typed throughout using Spark's `Aggregator` class. This class allows a Data Scientist to specify exactly what transformations worker nodes perform, how data is combined across nodes, and what each step should entail in terms of expected input and output type. The result is an intimate knowledge of how an aggregation transformation is being performed and an automated way to diagnose any typing issues that might arise. In other words, it's a foolproof way to save your bottle of beer next time you fly.

### When to use them

Deciding whether to perform type-safe aggregations on Spark Datasets can be a difficult decision. On the one hand, it's tempting to throw caution to the wind, declare the input and output types as `Dataset`, and call it a day. From my experience, this works for many of the exploratory and analytic instances a Data Scientist might encounter, as data and type validations can be performed on-the-fly in a notebook or scripting environment.

On the other hand, there are instances (e.g. automated ML pipelines in production, creating an API for other Data Scientists) when you need to ensure a transformation won't be yielding unexpected and potentially errant results without warning. These are the cases when you'll want to use the [Aggregator](https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/expressions/Aggregator.html) class in Spark. This class allows a Data Scientist to identify the input, intermediate, and output types when performing some type of custom aggregation.

I found Spark's `Aggregator` class to be somewhat confusing when I first encountered it. There are several steps, seemingly unused parameters, and limited examples online. Most of the examples I've seen tend to outline simple use-cases like summing a collection of integers or calculating an average. While these are certainly useful for tautological purposes, they don't offer much insight into other common aggregations that one might need to perform. Thus, my aim here is to demonstrate *two* uses of the `Aggregator` class that one might actually use in writing a data preprocessing application and break them down line-by-line.

## An `Aggregator` example

### Assumptions

Let's suppose you have the following retail data for a home improvement store

| customerID | productID | retailCategory |
|------------|-----------|----------------|
| 001        | zk67      | Lighting       |
| 001        | gg89      | Paint          |
| 002        | gg97      | Paint          |
| 003        | gd01      | Gardening      |
| 003        | af83      | A.C.           |
| 003        | af84      | A.C.           |
| 004        | gd77      | Gardening      |
| 004        | gd73      | Gardening      |
| 005        | cl55      | Cleaning       |
| 005        | zk67      | Lighting       |

customerID,productID,retailCate
001,zk67,Lighting  
001,gg89,Paint
002,gg97,Paint
003,gd01,Gardening
003,af83,A.C.
003,af84,A.C.
004,gd77,Gardening
004,gd73,Gardening
005,cl55,Cleaning  
005,zk67,Lighting

and want to do the following in a type-safe manner:

- provide a count of how many unique customers appear in each `retailCategory`
- collect an array of unique `productID`s, grouped by `retailCategory`
- transform the above data into a `Dataset` that contains the previous two steps

Let's also suppose we want to use the following general structure for implementing our aggregators:

```scala
case class SchemaBeforeTransform(...)

case class SchemaAfterTransform(...)

object DoSomething{
  def transformData = {
    val transformation = df
      .groupByKey(_.?)
      .agg(
        DoSomething.aggregator1,
        DoSomething.aggregator2
      ).map{
        case(...) => SchemaAfterTransform(...)
      }
  }

  val aggregator1{...}
  val aggregator2{...}
}
```

Which will result in a Data Scientist calling the following API when using our library:

```scala
val myValue = DoSomething.transformData(...)
```

Note that we are creating an object that contains the method we want to use to perform our aggregation, as well as both of our aggregators that we want to use within that method. This differs from many other examples I've seen online for two reasons. Firstly, we are attempting to create an API for a Data Scientist to use during a data preprocessing stage rather than creating a workflow to call with some `mainApp` object. Secondly, we are building more than two aggregators used by the same transformation.

### Prerequisites

The first items on our to do list are to import the `Aggregator` class, the `ExpressionEncoder` class, create a `case class` for the input data schema shown above, and create another `case class` for our output schema:

```scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

// input schema
case class RetailByCustomer(customerID: String,
                            productID: String,
                            retailCategory: String)

// output schema
case class AggRetailByCategory(retailCategory: String,
                               customerCount: Int,
                               productSet: Set[String]
```

!!! note
    We need to import the `ExpressionEncoder` class in order to define our own encoder for a `Set[String]` type, as this is not a default encoder in scala.

### Creating our Aggregators

Now we get to write our aggregators. I'll show both of them in full now and then break them down line-by-line.

```scala
// Counting unique customers in the dataset
val distinctCustomerCountAggregator: TypedColumn[RetailByCustomer, Int] =
  new Aggregator[RetailByCustomer, Set[String], Int] {
    override def zero: Set[String] = Set[String]()

    override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] =
      es + rbc.customerID

    override def merge(wx: Set[String], wy: Set[String]): Set[String] =
      wx.union(wy)

    override def finish(reduction: Set[String]): Int = reduction.size

    override def bufferEncoder: Encoder[Set[String]] =
      implicitly(ExpressionEncoder[Set[String]])

    override def outputEncoder: Encoder[Set[String]] =
      implicitly(Encoders.scalaInt)
  }

// Creating an array of productIDs from the dataset
val productIDAggregator: TypedColumn[RetailByCustomer, Set[String]] =
  new Aggregator[RetailByCustomer, Set[String], Set[String]] {
    override def zero: Set[String] = Set[String]()

    override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] =
      es + rbc.productID

    override def merge(wx: Set[String], wy: Set[String]): Set[String] =
      wx.union(wy)

    override def finish(reduction: Set[String]): Set[String] = reduction

    override def bufferEncoder: Encoder[Set[String]] =
      implicitly(ExpressionEncoder[Set[String]])

    override def outputEncoder: Encoder[Set[String]] =
      implicitly(ExpressionEncoder[Set[String]])
  }
```

As you might infer from the value name, our first aggregator provides a count of distinct customers. This is done by adding each customer to a `Set`, as sets only include one instance of any given value. It's also important to use a distinct count in this scenario in order to avoid overcounting in situations where incoming data may include duplicates or `productID` values that don't map to any `retailCategory`. Now, let's take a look at the first line:

```scala
val distinctCustomerCountAggregator: TypedColumn[RetailByCustomer, Int] =
  new Aggregator[RetailByCustomer, Set[String], Int] {
```

Here we are declaring a value that will return a [TypedColumn](https://spark.apache.org/docs/2.2.1/api/java/org/apache/spark/sql/TypedColumn.html) with the expected input (`RetailByCustomer` - an [Expression](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-Expression.html)) and the expected output (`Int` - an [ExpressionEncoder](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-ExpressionEncoder.html)). We're setting this value equal to a new `Aggregator` and defining the input type (`RetailByCustomer`), the intermediate type (`Set[String]`), and the output type (`Int`). That is, this Aggregator will expect to take in some type defined in our `RetailByCustomer` case class, perform some type of transformation that will result in a `Set[String]` type, and return some value of type `Int` after aggregation.

Now onto the meat of the function. If you're reading this article, you probably know that Spark is a framework for performing data transformations in a distributed fashion, so you probably know that there is a master node that sends out tasks to worker node executors to perform data transformations. But what tasks do you send to the worker nodes to work together when performing an aggregation across the entire dataset?

The way I like to think about it at a *high level* starts with individual worker nodes. The `Aggregator` class sends tasks to an executor on an individual worker node (and all other worker nodes active for the job) on how to begin an aggregation:

```scala
override def zero: Set[String] = Set[String]()
```

That is, in our case, each worker node should start the aggregation with an empty set of type `String`.

Next, the `Aggregator` class tells each worker node what to do with the data it has in memory:

```scala
override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] =
  es + rbc.customerID
```

The instructions in our `reduce` definition are telling each worker node to expect inputs of `es`, our "empty set" from the previous step, and `rbc`, our `RetailByCustomer` case class defined above. Additionally, the output of this transformation on each worker node is going to result in an expected type `Set[String]`. Lastly, the actual transformation to take place on each worker node is defined. Each in-memory value stored as a type `RetailByCustomer.customerID` (which we defined as a `String`) is added to the existing empty set, resulting in a `Set[String]` containing distinct customer IDs.

To see this in practice, suppose our data has been partitioned by `retailCategory`, such that information about "Lighting" and "Gardening" is stored on one worker node, "A.C." and "Cleaning" are stored on a second node, and "Paint" is stored on a third node. The resulting sets on each node should look like:

|                      | Worker 1          | Worker 2          | Worker 3       |
|----------------------|-------------------|-------------------|----------------|
| Input `es`           | Set()               | Set()                             | Set() |
| Input `rbc`          | "001", "005", "003", "004", "004" | "003", "003", "005" | "001", "002" |
| Output `Set[String]` | Set("001", "005", "003", "004")   | Set("003", "005")   | Set("001", "002") |

Now that the transformation of finding unique `customerID`s has been performed on each worker node, the `Aggregator` class has to instruct the worker nodes how to interact with each other in order to find unique `customerID`s *across* nodes. This brings us to the `merge` definition:

```scala
override def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
```

Here, we are defining the two expected inputs as type `Set[String]`; one from "Worker X" (`wx`) and one from "Worker Y" (`wy`). Again, we define the expected output type as `Set[String]`. Lastly, the `Aggregator` class has to give instructions on what each worker node should do when interacting with the others. In this case, we tell each worker to union the output from the `reduce` step with the other worker's `reduce` output (i.e. return all elements between the two sets).

While there's much more nuance to how information is passed across worker nodes, for the purposes of understanding the `Aggregator` class we can visualize the output of the `merge` step as follows:

|          | Worker 1 | Worker 2 | Worker 3 |
|----------|----------|----------|----------|
| Worker 1 | - | Set("001", "005", "003", "004") | Set("001", "005", "003", "004", "002") |
| Worker 2 | - | - | Set("003", "005", "001", "002") |
| Worker 3 | - | - | - |

which will result in the output `Set("001", "005", "003", "004", "002")` of type `Set[String]`.

The next step, `finish`, provides instructions for the driver to perform an action:

```scala
override def finish(reduction: Set[String]): Int = reduction.size
```

Here, we define the expected input type as our unioned `Set[String]` and expected output type as `Int`. We also define the action to be taken on the `Set[String]` input by calling `size`, which returns an integer representing the number of items in the set. In the case of our example, this returns `5`.

The last step is to define the `Encoder` for both our buffer (or intermediate) step and output step:

```scala
override def bufferEncoder: Encoder[Set[String]] =
  implicitly(ExpressionEncoder[Set[String]])

override def outputEncoder: Encoder[Int] =
  implicitly(Encoders.scalaInt)
```

The output for `distinctCustomerCountAggregator` is an `Int`, which has a predefined encoder in Scala. Thus, we can simply call `Encoders.scalaInt` when defining our `outputEncoder` and pass it implicitly to the other definitions. However, although both `Set` and `String` have predefined encoders in Scala, `Set[String]` does not. Therefore, we have to define our own `Set[String]` encoder using the `ExpressionEncoder` class that was imported in the first step.

`productIDAggregator` is actually quite similar to the `distinctCustomerCountAggregator` except for one key difference: we are expecting an output of type `Set[String]` rather than `Int`. This will change a couple of the steps.

!!! Exercise
    I encourage you to think which steps will need to be changed in order to facilitate this before continuing (hint: there are 3).

The first difference involves changing both the `TypedColumn` and expected `Aggregator` output types to `Set[String]`:

```scala
val productIDAggregator: TypedColumn[RetailByCustomer, Set[String]] =
  new Aggregator[RetailByCustomer, Set[String], Set[String]]
```

The next three steps are actually the same for both aggregators: we instruct the worker nodes to create empty sets, add `productID`s to those sets, and union the resulting sets across themselves.

The second difference, however, appears in the `finish` definition. We want to change the expected output type from `Int` to `Set[String]` and eliminate the `size` method, as we just want to return the actual set of `productID`s:

```scala
override def finish(reduction: Set[String]): Set[String] = reduction
```

Lastly, we need to change the `outputEncoder`. Since we're not really transforming between types in this case, our `bufferEncoder` and `outputEncoder` will be the same:

```scala
override def bufferEncoder: Encoder[Set[String]] =
  implicitly(ExpressionEncoder[Set[String]])

override def outputEncoder: Encoder[Set[String]] =
  implicitly(ExpressionEncoder[Set[String]])
```

And that's it! We've created two aggregators that perform two distinct functions!

### Using our Aggregators

To use these in practice, let's take a look at the full code:

```scala
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

// input schema
case class RetailByCustomer(customerID: String, productID: String, retailCategory: String)

// output schema
case class AggRetailByCategory(retailCategory: String, customerCount: Int, productSet: Set[String]

// object that holds our specific data transformation method
object PreprocessData{
  // data transformation we want to perform
  // note the typing of the df parameter and expected output
  def createArrayAndCount(df: Dataset[RetailByCustomer]): Dataset[AggRetailByCategory] = {
    val transformedData: Dataset[AggRetailByCategory] =
      // using a groupByKey method to group by any Dataset with type defined as 'retailCategory'
      df.groupByKey(_.retailCategory)
      .agg(
        // our aggregator functions at work
        PreprocessData.distinctCustomerCountAggregator.name("customerCount"),
        DoSomething.productIDAggregator.name("productIDs")
      ).map{
        // retailCategory(rc), customerCount(cc), productSet(ps)
        case(rc: String, cc: Int, ps: Set[String]) => AggRetailByCategory(rc, cc, ps)
      }
    transformedData
  }

  val distinctCustomerCountAggregator: TypedColumn[RetailByCustomer, Int] = new Aggregator[RetailByCustomer, Set[String], Int] {
    override def zero: Set[String] = Set[String]()
    override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] = es + rbc.customerID
    override def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
    override def finish(reduction: Set[String]): Int = reduction.size
    override def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    override def outputEncoder: Encoder[Set[String]] = implicitly(Encoders.scalaInt)
    }.toColumn

  // Creating an array of productIDs from the dataset
  val productIDAggregator: TypedColumn[RetailByCustomer, Set[String]] = new Aggregator[RetailByCustomer, Set[String], Set[String]] {
    override def zero: Set[String] = Set[String]()
    override def reduce(es: Set[String], rbc: RetailByCustomer): Set[String] = es + rbc.productID
    override def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
    override def finish(reduction: Set[String]): Set[String] = reduction
    override def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    override def outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    }.toColumn
}
```

After publishing the code above, a Data Scientist can call the following when building a data transformation pipeline:

```scala
import com.wherever.code.is.PreprocessData

val retailData = Seq(
("001", "zk67", "Lighting"),
("001", "gg89", "Paint"),
("002", "gg97", "Paint"),
("003", "gd01", "Gardening"),
("003", "af83", "A.C."),
("003", "af84", "A.C."),
("004", "gd77", "Gardening"),
("004", "gd73", "Gardening"),
("005", "cl55", "Cleaning"),
("005", "zk67", "Lighting")
).toDF("customerID", "productID", "retailCategory")

val transformedRetailData = PreprocessData.createArrayAndCount(retailData)
```

where `val transformedRetailData` should look something like

| retailCategory | customerCount| productSet |
|----------------|:------------:|------------|
| Lighting       | 2            | Set("zk67")                 |
| Gardening      | 2            | Set("gd01", "gd77", "gd73") |
| Paint          | 2            | Set("gg89", "gg97")         |
| A.C.           | 1            | Set("af83", "af84")         |
| Cleaning       | 1            | Set("cl55")                 |

## Conclusions

- Type-safe aggregations allow Data Scientists to specify how transformations are being undertaken, how data is combined within and across nodes, and what each step should entail in terms of expected input and output type.
- Use Spark's `Aggregator` class to perform type-safe transformations. The typical use-case is in a production-level environment, writing an API, or when you plan on repeated use of an aggregation.
- Understanding what is happening under the hood in Spark at a high level is important in understanding how the `Aggregator` class works.

## Reaching out

Thanks for reading this article! Please feel free to reach out with any comments/questions via [Twitter](https://twitter.com/martainyo) or by commenting on the [Medium Post](https://medium.com/@mfjackson/using-the-spark-aggregator-class-in-scala-341ee8bb46e5?sk=9b3f3057bd6c0d6ecca8a830921aa4d2). I'm constantly learning and looking to improve my knowledge of all things Data Science, so if I've made any erroneous statements or you know of a better methodology than the one in this article, please let me know!!

<div class="footer">
&copy; 2020 Martin Jackson
</div>