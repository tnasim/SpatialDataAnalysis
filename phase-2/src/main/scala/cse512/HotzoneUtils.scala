package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    //ST_Contains code from phase 1
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

}