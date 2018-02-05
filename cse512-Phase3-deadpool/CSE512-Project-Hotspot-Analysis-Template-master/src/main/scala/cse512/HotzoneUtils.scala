package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
var r=new Array[String](4)
		var p=new Array[String](2)
		p=pointString.split(",")
		r=queryRectangle.split(",")
		var b1:Boolean = true;
		var x1:Double =0.0
		var x2:Double =0.0
		var y1:Double =0.0
		var y2:Double =0.0
		var p0:Double =0.0
		var p1:Double =0.0
		p0=(p(0)).toDouble
		p1=(p(1)).toDouble
		var b=1
		if(r(0).toDouble>r(2).toDouble)
		{
			x1=r(0).toDouble
			x2=r(2).toDouble
		}
		else
		{
			x2=r(0).toDouble
			x1=r(2).toDouble
			
		}
		if(r(1).toDouble>r(3).toDouble)
		{
			y1=r(1).toDouble
			y2=r(3).toDouble
		}
		else
		{
			y2=r(1).toDouble
			y1=r(3).toDouble
			
		}
		if(p0<=x1 && p0>=x2 && p1<=y1 && p1>=y2)
		  return true

		else
		  return false

    // YOU NEED TO CHANGE THIS PART
   // return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
