package cse512

import org.apache.spark.sql.SparkSession
import math.{ sqrt, pow,toRadians,sin,cos,atan2 }

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(({
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
		  b1=true
		else
		  b1=false
		b1

	})))

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
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(({
		var r=new Array[String](4)
		var p=new Array[String](2)
		r=queryRectangle.split(",")
		p=pointString.split(",")
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
		  b1=true
		else
		  b1=false
		b1

	})))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(({
var p1=new Array[String](2)
var p2=new Array[String](2)
p1 = pointString1.split(",")
p2 = pointString2.split(",")
var f1:Double = p2(0).toDouble - p1(0).toDouble
var f2:Double = p2(1).toDouble - p1(1).toDouble
var f3:Double = (f1*f1) + (f2*f2)
var res:Double = sqrt(f3)

if(res <= distance )
	true
else
	false
})))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(({
var p1=new Array[String](2)
var p2=new Array[String](2)
p1 = pointString1.split(",")
p2 = pointString2.split(",")
var f1:Double = p2(0).toDouble - p1(0).toDouble
var f2:Double = p2(1).toDouble - p1(1).toDouble
var f3:Double = (f1*f1) + (f2*f2)
var res:Double = sqrt(f3)

if(res <= distance )
	true
else
	false
})))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
