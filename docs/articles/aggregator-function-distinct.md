# Using the Spark Aggregator class in Scala

*Tags: Spark, Scala, Aggregator, Machine Learning, Dataset, DataFrame, Count Distinct*

## When to use Type-safe transformations

Deciding whether to perform type-safe transformations on Spark Datasets can be a difficult decision. On the one hand, it's tempting to throw caution to the wind, declare the input and output types as `Dataset`, and call it a day. From my experience, this works for many of the exploratory and analytic instances a Data Scientist might encounter, as data and type validations can be performed on-the-fly in a notebook or scripting environment.

On the other hand, there are instances (e.g. automated ML pipelines in production) when you need to ensure a transformation won't be yielding unexpected and potentially errant results without warning. These are the cases when you'll want to use the [Aggregator](https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/expressions/Aggregator.html) class in Spark. This class allows a Data Scientist to identify the input, intermediate, and output types when performing some type of custom aggregation.

I found Spark's `Aggregator` class to be somewhat confusing when I first encountered it. There are several steps, seemingly unused parameters, and limited examples online. Most of the examples I've seen tend to outline simple use-cases like summing a collection of integers. Thus, my aim here is to demonstrate *two* uses of the `Aggregator` class that one might actually use a Data Scientist and break them down line-by-line.

## An `Aggregator` example

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

and want to do the following in a type-safe manner:

- provide a count of how many unique customers appear in each `retailCategory`
- collect an array of unique `productID`s, grouped by `retailCategory`
- transform the above data into a `Dataset` that contains the previous two steps

The first items on our to do list are to import the `Aggregator` class, the `ExpressionEncoder` class, create a `case class` for the input data schema shown above, and create another `case class` for our output schema:

```scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

// input schema
case class RetailByCustomer(customerID: String, productID: String, retailCategory: String)

// output schema
case class AggRetailByCategory(retailCategory: String, customerCount: Int, productArray: Set[String]
```

Note: We need to import the `ExpressionEncoder` class in order to define our own encoder for a `Set[String]` type, as this is not a default encoder in scala.

Now we get to write our aggregators. I'll show both of them in full now and then break them down line-by-line.

```scala
// Counting unique customers in the dataset
val uniqueCustomerCountAggregator: TypedColumn[RetailByCustomer, Int] = new Aggregator[RetailByCustomer, Set[String], Int] {
  override def zero: Set[String] = Set[String]()
  override def reduce(zs: Set[String], rbc: RetailByCustomer): Set[String] = zs + rbc.customerID
  override def merge(w1: Set[String], w2: Set[String]): Set[String] = w1.union(w2)
  override def finish(reduction: Set[String]): Int = reduction.size
  override bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
  override outputEncoder: Encoder[Set[String]] = implicitly(Encoders.scalaInt)
  }

// Creating an array of productIDs from the dataset
val productIDAggregator: TypedColumn[RetailByCustomer, Set[String]] = new Aggregator[RetailByCustomer, Set[String], Set[String]] with Serializable {
  override def zero: Set[String] = Set[String]()
  override def reduce(zs: Set[String], rbc: RetailByCustomer): Set[String] = zs + rbc.productID
  override def merge(w1: Set[String], w2: Set[String]): Set[String] = w1.union(w2)
  override def finish(reduction: Set[String]): Int = reduction
  override bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
  override outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
  }
```
