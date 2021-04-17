package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

 
  pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " AND y >= " + minY + " AND z >= " + minZ + " AND x <= " + maxX + " AND y <= " + maxY + " AND z <= " + maxZ).orderBy("z", "y", "x")
  pickupInfo.createOrReplaceTempView("pickUpInfo")

  val dfNumberOfPointsInCell = spark.sql("SELECT P.x, P.y, P.z, count(*) AS xCount FROM pickUpInfo AS P GROUP BY P.z, P.y, P.x ORDER BY P.z, P.y, P.x")
    dfNumberOfPointsInCell.createOrReplaceTempView("Cells")
  

  spark.udf.register("isNeighbour", (Cell1X: Int, Cell1Y: Int, Cell1Z: Int, Cell2X: Int, Cell2Y: Int, Cell2Z: Int) => HotcellUtils.isNeighbour(Cell1X, Cell1Y, Cell1Z, Cell2X, Cell2Y, Cell2Z))
  val neighbor = spark.sql("SELECT C1.x, C1.y, C1.z, SUM(C2.xCount) as sum_x, COUNT(C2.xCount) as W FROM Cells C1, Cells C2 WHERE isNeighbour(C1.x,C1.y,C1.z,C2.x,C2.y,C2.z) GROUP BY C1.x, C1.y, C1.z ")
  neighbor.createOrReplaceTempView("Neighbor")
  neighbor.show()

  val sum_x = spark.sql("SELECT SUM(Cells.xCount) FROM Cells").first().getLong(0).toDouble
  var X = sum_x/(numCells*1.0)
  val sum_x_pow_2 = spark.sql("SELECT SUM(Cells.xCount*Cells.xCount) FROM Cells").first().getLong(0).toDouble
  var S = Math.sqrt((sum_x_pow_2/numCells*1.0) - X*X)

  spark.udf.register("g_score", (sum_x: Double, W: Double) =>HotcellUtils.g_score(sum_x, W, X , S, numCells))
  val top_50_gscore = spark.sql("SELECT x, y, z,score from (SELECT x, y, z, g_score(Neighbor.sum_x, Neighbor.W) as score FROM Neighbor ORDER BY score DESC limit 50)")
  top_50_gscore.show()

  return top_50_gscore
  

}
}
