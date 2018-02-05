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
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
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
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*).cache()
    //pickupInfo.show()
    pickupInfo.createOrReplaceTempView("pickupInfo")
    // Define the min and max of x, y, z
    val coordinateStep = 0.01
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    spark.udf.register("NeighbourCount",(x: Int, y: Int, z:Int)=>((
     HotcellUtils.NeighbourCount(x,y,z)
    )))

    spark.udf.register("isNeighbour",(x1: String, y1 :String, z1: String, x2: String, y2: String, z2: String)=>((
      HotcellUtils.isNeighbour(x1, y1, z1, x2, y2, z2)
    )))
    

    var query = "select x,y,z,count(*) as meanCalc,count(*) as stDevCalc from pickupInfo where x>=" + minX.toString + " AND x<=" + maxX.toString + " AND y>=" + minY.toString + " AND y<=" + maxY.toString + " AND z>=" + minZ.toString + " AND z<=" + maxZ.toString + " GROUP BY x,y,z"
   
    var selectDataFrame = "select x,y,z from DataframeJoin ORDER BY GScore DESC"
    var joinDataframe = "select x,y,z,first(meanCalc) as meanCalc, first(stdevPoints) as stdevPoints, sum(p) as neighboursSum, NeighbourCount(x,y,z) as neighboursCount from filterDF GROUP BY x,y,z"
    var selectPoints = "select x as x1,y as y1 ,z as z1,stDevCalc as p from pickUpEntry SORT BY x,y,z"
    
    var pickUpEntry = spark.sql(query)
    
    //find the mean and standard deviation
    val meanSum = pickUpEntry.select(sum("meanCalc")).first().getLong(0).toDouble
    pickUpEntry = pickUpEntry.withColumn("meanCalc", lit(meanSum)/lit(numCells)).withColumn("stdevPoints", col("stDevCalc") * col("stDevCalc"))
    var stdDev = Math.sqrt((pickUpEntry.select(sum("stdevPoints")).first().getLong(0).toDouble / numCells) - ((meanSum/numCells)*(meanSum/numCells)))
    pickUpEntry = pickUpEntry.withColumn("stdevPoints", lit(stdDev)).sort("x").sort("y").sort("z").persist()
    pickUpEntry.createOrReplaceTempView("pickUpEntry")
    
    val subEntry = spark.sql(selectPoints).persist()
    subEntry.createOrReplaceTempView("subEntry")

    pickUpEntry = pickUpEntry.select("*").alias("table1").join(subEntry.select("*").alias("table2"), (pickUpEntry("x") === subEntry("x1") -1 || pickUpEntry("x") === subEntry("x1") || pickUpEntry("x") === subEntry("x1") +1)
      && (pickUpEntry("y") === subEntry("y1") - 1 || pickUpEntry("y") === subEntry("y1") || pickUpEntry("y") === subEntry("y1") + 1)
      && (pickUpEntry("z") === subEntry("z1") -1 || pickUpEntry("z") === subEntry("z1") || pickUpEntry("z") === subEntry("z1") + 1)).persist()
    pickUpEntry.createOrReplaceTempView("pickUpEntry")

    var DataframeJoin = spark.sql("select x,y,z,first(meanCalc) as meanCalc, first(stdevPoints) as stdevPoints, sum(p) as neighboursSum, NeighbourCount(x,y,z) as neighboursCount from pickUpEntry GROUP BY x,y,z")

    // calculate gscore
    DataframeJoin = DataframeJoin.withColumn("numerator",col("neighboursSum")-col("meanCalc")*col("neighboursCount")).withColumn("denominator",col("stdevPoints")*sqrt((col("neighboursCount")*numCells-col("neighboursCount")*col("neighboursCount"))/numCells-1)).withColumn("GScore",col("numerator")/col("denominator")).persist()
    DataframeJoin.createOrReplaceTempView("DataframeJoin")
    return spark.sql(selectDataFrame)
  }
}
