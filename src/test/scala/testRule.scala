import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Range}

import scala.reflect.internal.Positions
object testRule {

  case object IntervalJoin extends Strategy with Serializable {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Join(Range(start1, end1, 1, part1, Seq(o1),false),
      Range(start2, end2, 1, part2, Seq(o2),false),
      Inner, Some(EqualTo(e1, e2)))
        if ((o1 semanticEquals e1) && (o2 semanticEquals e2)) ||
          ((o1 semanticEquals e2) && (o2 semanticEquals e1)) =>
        if ((end2 >= start1) && (end2 <= end2)) {
          val start = math.max(start1, start2)
          val end = math.min(end1, end2)
          val part = math.max(part1.getOrElse(200), part2.getOrElse(200))
          val result = RangeExec(Range(start, end, 1, Some(part), o1 :: Nil,false))
          val twoColumns = ProjectExec(
            Alias(o1, o1.name)(exprId = o1.exprId) :: Nil,
            result)
          twoColumns :: Nil
        }
        else {
          Nil
        }
      case _ => Nil


    }
  }



  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().master("local")
      .config(new SparkConf())
      .getOrCreate()
    spark.experimental.extraStrategies = IntervalJoin :: Nil

    val begin =System.currentTimeMillis()
    val tableA = spark.range(20000000).as('a)
    val tableB = spark.range(10000000).as('b)

    val result= tableA.join(tableB,Seq("id"))
      .groupBy()
      .count()
    result.show()
    val end=System.currentTimeMillis()
    val cost =end - begin
    println("time is " +cost)
    result.explain()


  }

}
