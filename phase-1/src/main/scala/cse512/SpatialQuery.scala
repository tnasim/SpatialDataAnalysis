package cse512

import org.apache.spark.sql.SparkSession

import scala.math.{pow, sqrt}

object SpatialQuery extends App{
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if (queryRectangle == null || pointString == null || pointString.isEmpty()|| queryRectangle.isEmpty())
      return false

    val rectSplitArray = queryRectangle.split(",").map(_.toDouble)
    val topLeftX = rectSplitArray(0)
    val topLeftY = rectSplitArray(1)
    val botRgtX = rectSplitArray(2)
    val botRgtY = rectSplitArray(3)

    val pointStringSplitArray = pointString.split(",").map(_.toDouble)
    val pointX = pointStringSplitArray(0)
    val pointY = pointStringSplitArray(1)

    if ((pointX >= topLeftX && pointX <= botRgtX && pointY >= topLeftY && pointY <= botRgtY) ||
      (pointX >= botRgtX && pointX <= topLeftX && pointY >= botRgtY && pointY <= topLeftY)) {
      return true
    }
    return false
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    if(pointString1 == null || pointString2 ==null || !(distance > 0))
      return false
    val p1Split = pointString1.split(",").map(_.toDouble)
    val p2Split = pointString2.split(",").map(_.toDouble)
    if(p1Split.length !=2 || p2Split.length !=2)
      return false

    return (sqrt(pow(p1Split(0) - p2Split(0), 2) + pow(p1Split(1) - p2Split(1), 2)) <= distance)
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle,pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>ST_Contains(queryRectangle,pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>ST_Within(pointString1, pointString2, distance))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>ST_Within(pointString1, pointString2, distance))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
