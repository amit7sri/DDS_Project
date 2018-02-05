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

  def isNeighbour (x1 : String,y1 : String, z1 : String, x2 : String, y2 : String, z2 : String): Boolean =
  {
    var x11 = x1.toInt
    var y11 = y1.toInt
    var z11 = z1.toInt
    var x22 = x2.toInt
    var y22 = y2.toInt
    var z22 = z2.toInt
    return (x11 == x22 -1 || x11 == x22 || x11 == x22 +1) && (y11 == y22 -1 || y11 == y22 || y11 == y22 +1) && (z11 == z22 -1 || z11 == z22 || z11 == z22 +1);
  }

  def NeighbourCount(x: Double, y: Double, z: Double): Int ={
    val minX1 = -74.50/coordinateStep
    val maxX1 = -73.70/coordinateStep
    val minY1 = 40.50/coordinateStep
    val maxY1 = 40.90/coordinateStep
    val minZ1 = 1
    val maxZ1 = 31
    var noOfBoundaries = 0
    
    if(x==minX1 || x==maxX1)
      noOfBoundaries+=1
    if(y==minY1 || y==maxY1)
      noOfBoundaries+=1
    if(z==minZ1 || z==maxZ1)
      noOfBoundaries+=1

    if (noOfBoundaries == 1)
      return 17
    else if (noOfBoundaries == 2)
      return 11
    else if (noOfBoundaries == 3)
      return 7
    else
      return 27
  }
}
