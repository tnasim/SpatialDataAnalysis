package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART

    //rectangle (x1,y1,x2,y2) coordinates
    val rectangleVal = queryRectangle.split(",")
    val x1 = rectangleVal(0).toDouble
    val y1 = rectangleVal(1).toDouble
    val x2 = rectangleVal(2).toDouble
    val y2 = rectangleVal(3).toDouble

    val minX = math.min(x1,x2)
    val minY = math.min(y1,y2)
    val maxX = math.max(x1,x2)
    val maxY = math.max(y1,y2)

    //point (a,b) coordinates
    val pointStringVal = pointString.split(",")
    val pointA = pointStringVal(0).toDouble
    val pointB = pointStringVal(1).toDouble

    if(minX <= pointA && maxX >= pointA && minY <= pointB && maxY >= pointB) {
      return true
    }

    else {
      return false
    }

  }

}