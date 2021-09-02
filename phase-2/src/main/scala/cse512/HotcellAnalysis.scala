package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.FATAL)
  Logger.getLogger("org.apache").setLevel(Level.FATAL)
  Logger.getLogger("akka").setLevel(Level.FATAL)
  Logger.getLogger("com").setLevel(Level.FATAL)

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

    // YOU NEED TO CHANGE THIS PART

    pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " AND y >= " + minY + " AND z >= " + minZ + " AND x <= " + maxX + " AND y <= " + maxY + " AND z <= " + maxZ).orderBy("z", "y", "x")
    pickupInfo.createOrReplaceTempView("pickUpInfo")
    /**
     * This DataFrame contains coordinates for each Cell
     *  and the number of points (points_in_cell) for each Cell
     */
    val dfNumberOfPointsInCell = spark.sql("SELECT P.x, P.y, P.z, count(*) AS points_in_cell "
      + "FROM pickUpInfo AS P "
      + "GROUP BY P.z, P.y, P.x "
      + "ORDER BY P.z, P.y, P.x" )
    dfNumberOfPointsInCell.createOrReplaceTempView("Cells")

    /** Average number of points in all the Cells */
    val avg = spark.sql(
      "SELECT avg(C.points_in_cell) FROM Cells AS C"
    ).first().getDouble(0)

    /** Standard Deviation of the number of points in all the Cells */
    val stdDev = spark.sql(
      "SELECT std(C.points_in_cell) FROM Cells AS C"
    ).first().getDouble(0)


    spark.udf.register("CountNeighbor", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
        => ((HotcellUtils.numOfNeighbours(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))))
//    val dfAdjacentHotcell = spark.sql(
//      "SELECT CountNeighbor("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "C1.x,C1.y,C1.z) as W, "
//        + "count(*) as countall, "
//        + "C1.x as x,C1.y as y,C1.z as z, "
//        + "sum(C2.points_in_cell) as sum_WX "
//        + "FROM Cells AS C1, Cells AS C2 "
//        + "WHERE (C2.y = C1.y+1 OR C2.y = C1.y OR C2.y = C1.y-1) "
//        +   "AND (C2.x = C1.x+1 OR C2.x = C1.x OR C2.x = C1.x-1) "
//        +   "AND (C2.z = C1.z+1 OR C2.z = C1.z OR C2.z = C1.z-1) "
//        + "GROUP BY C1.z, C1.y, C1.x "
//        + "ORDER BY C1.z, C1.y, C1.x"
//    )

    val dfAdjacentHotcell = spark.sql(
      "SELECT count(*) as W, "
        + "C1.x as x,C1.y as y,C1.z as z, "
        + "sum(C2.points_in_cell) as sum_WX "
        + "FROM Cells AS C1, Cells AS C2 "
        + "WHERE (C2.y = C1.y+1 OR C2.y = C1.y OR C2.y = C1.y-1) "
        +   "AND (C2.x = C1.x+1 OR C2.x = C1.x OR C2.x = C1.x-1) "
        +   "AND (C2.z = C1.z+1 OR C2.z = C1.z OR C2.z = C1.z-1) "
        + "GROUP BY C1.z, C1.y, C1.x "
        + "ORDER BY C1.z, C1.y, C1.x"
    )

    // Define function for G-score calculation
    val fnCalculateGScore = udf(
      (numCells: Int , x: Int, y: Int, z: Int, W: Int, sum_WX: Int, avg: Double, stdDev: Double)
        => HotcellUtils.calculateGScore(numCells, x, y, z, W, sum_WX, avg, stdDev)
    )

    val dfWithGScore = dfAdjacentHotcell.withColumn(
        "GScore",
        fnCalculateGScore(
          lit(numCells),
          col("x"), col("y"), col("z"),
          col("W"), col("sum_WX"),
          lit(avg), lit(stdDev)
        )
    ).orderBy(desc("GScore")).limit(50)

    pickupInfo = dfWithGScore.select(col("x"), col("y"), col("z"), col("GScore") )
    pickupInfo.show()
    return pickupInfo
  }
}
