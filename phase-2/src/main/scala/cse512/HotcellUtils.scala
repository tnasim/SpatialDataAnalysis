package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def numOfNeighbours(minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, X: Int, Y: Int, Z: Int): Int =
  {
    var count_edge = 0;

    if (X == minX || X == maxX)
      count_edge += 1;

    if (Y == minY || Y == maxY)
      count_edge += 1;

    if (Z == minZ || Z == maxZ)
      count_edge += 1

    
    if (count_edge == 1) // edge on one side
      return 17
    else if (count_edge == 2) // edge on 2 sides
      return 11
    else if (count_edge == 3) // edge on 3 sides
      return 7

    // not on the edge of the cube
    return 26
  }

  def calculateGScore(numCells: Int, x: Int, y: Int, z: Int, W: Int, sum_WX: Int, avg: Double, stdDev: Double): Double = {
    var dW: Double = W.toDouble
    var N: Double = numCells.toDouble
    (
      (sum_WX.toDouble - (avg * dW))
        /
      (
        stdDev *
          math.sqrt
          (
            (( N * dW ) - (dW * dW))
              /
            (N - 1.0)
          )
      )
    )
  }
}
