package edu.stanford.cs245

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral, create}
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, DecimalLiteral, EmptyRow, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.EqualNullSafe

import scala.math.pow
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType}

object Transforms {

  // Check whether a ScalaUDF Expression is our dist UDF
  def isDistUdf(udf: ScalaUDF): Boolean = {
    udf.udfName.getOrElse("") == "dist"
  }

  // Get an Expression representing the dist_sq UDF with the provided
  // arguments
  def getDistSqUdf(args: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      (x1: Double, y1: Double, x2: Double, y2: Double) => {
        val xDiff = x1 - x2
        val yDiff = y1 - y2
        xDiff * xDiff + yDiff * yDiff
      }, DoubleType, args, Seq(DoubleType, DoubleType, DoubleType, DoubleType),
      udfName = Some("dist_sq"))
  }

  // Return any additional optimization passes here
  def getOptimizationPasses(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(EliminateZeroDists(spark), ConvertRhsDists(spark), SimplifyBooleanDists(spark), SimplifyZeroDists(spark), SimplifyCmpDists(spark), SimplifySortDists(spark))
  }

  def toDouble(literal: Literal): Double = {
    literal match {
      case Literal(value: Any, dataType: ShortType) => literal.eval().asInstanceOf[Short].toDouble
      case Literal(value: Any, dataType: IntegerType) => literal.eval().asInstanceOf[Int].toDouble
      case Literal(value: Any, dataType: LongType) => literal.eval().asInstanceOf[Long].toDouble
      case Literal(value: Any, dataType: FloatType) => literal.eval().asInstanceOf[Float].toDouble
      case Literal(value: Any, dataType: DoubleType) => literal.eval().asInstanceOf[Double]
      case _ => Double.NaN
    }
  }

  // https://docs.scala-lang.org/style/method-invocation.html
  // https://www.scala-lang.org/files/archive/spec/2.13/08-pattern-matching.html#pattern-matching-anonymous-functions
  // Go through expression
  // https://www.scala-lang.org/files/archive/spec/2.13/08-pattern-matching.html#variable-patterns
  case class EliminateZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case udf: ScalaUDF if isDistUdf(udf) && udf.children(0) == udf.children(2) &&
        udf.children(1) == udf.children(3) => Literal(0.0, DoubleType)
    }
  }

  // Make dist(...) always be on left hand side when doing BinaryComparison
  // Convert 123 > dist(...) into dis(...) <= 123
  case class ConvertRhsDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case expr @ (operand) GreaterThan (udf: ScalaUDF) if isDistUdf(udf) =>
        LessThan(udf, operand)
      case expr @ (operand) GreaterThanOrEqual (udf: ScalaUDF) if isDistUdf(udf) =>
        LessThanOrEqual(udf, operand)
      case expr @ (operand) LessThan (udf: ScalaUDF) if isDistUdf(udf) =>
        GreaterThan(udf, operand)
      case expr @ (operand) LessThanOrEqual (udf: ScalaUDF) if isDistUdf(udf) =>
        GreaterThanOrEqual(udf, operand)
    }
  }

  case class SimplifyBooleanDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case expr @ (udf: ScalaUDF) GreaterThan (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) < 0.0 =>
        println("SimplifyBooleanDists dist > (negative)", expr)
        TrueLiteral

      case expr @ (udf: ScalaUDF) GreaterThanOrEqual (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) <= 0.0 =>
        println("SimplifyBooleanDists dist >= (non-negative)", expr)
        TrueLiteral

      case expr @ (udf: ScalaUDF) LessThan (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) <= 0.0 =>
        println("SimplifyBooleanDists dist < (non-positive)", expr)
        FalseLiteral

      case expr @ (udf: ScalaUDF) LessThanOrEqual (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) < 0.0 =>
        println("SimplifyBooleanDists dist <= (negative)", expr)
        FalseLiteral

      case expr @ (udf: ScalaUDF) EqualTo (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) < 0.0 =>
        println("SimplifyBooleanDists dist == (negative)", expr)
        FalseLiteral

      case expr @ expressions.EqualNullSafe(udf: ScalaUDF, literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) < 0.0 =>
        println("SimplifyBooleanDists dist <=> (negative)", expr)
        FalseLiteral
    }
  }

  case class SimplifyZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case expr @ (udf: ScalaUDF) EqualTo (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN && toDouble(literal) == 0.0 =>
        println("SimplifyZeroDists dist == 0", expr)
        And(EqualTo(udf.children(0), udf.children(2)), EqualTo(udf.children(1), udf.children(3)))
     }
  }

  case class SimplifyCmpDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case expr @ (udf: ScalaUDF) BinaryComparison (literal: Literal)
        if isDistUdf(udf) && !toDouble(literal).isNaN =>
        println("SimplifyCmpDists dist cmp literal", expr)
        expr.withNewChildren(Seq(getDistSqUdf(udf.children), Literal.create(pow(toDouble(literal), 2), DoubleType)))

      case expr @ (udf1: ScalaUDF) BinaryComparison (udf2: ScalaUDF)
        if isDistUdf(udf1) && isDistUdf(udf2) =>
        println("SimplifyCmpDists dist cmp dist", expr)
        expr.withNewChildren(Seq(getDistSqUdf(udf1.children), getDistSqUdf(udf2.children)))
    }
  }

  case class SimplifySortDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Transform a plan that match our case (a Sort plan)
      case sortPlan @ Sort(orders, _, _) =>
        // Next, for that plan, check every SortOrder
        // If SortOrder.child (an Expression) is distUdf then turn it into distSqUdf
        // Return a new sort plan with a bunch of new SortOrders
        val newOrders = orders.map((sortOrder: SortOrder) => sortOrder.child match {
          case expr @ (udf: ScalaUDF) if isDistUdf(udf) =>
            println("SimplifySortDists dist ", expr)
            sortOrder.copy(child = getDistSqUdf(udf.children))

          case expr => sortOrder
        })
        sortPlan.copy(order = newOrders)
    }
  }
}
