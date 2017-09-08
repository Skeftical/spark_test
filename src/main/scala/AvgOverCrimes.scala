import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 31/08/17.
  */
object AvgOverCrimes {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Generate Query Dataset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = spark.read.option("header","true").option("inferSchema", "true").csv("/home/fotis//DATA/Crimes_-_2001_to_present.csv").persist()

    val format = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")
    //Get time to update a case
    val time_update_case = df.toDF().na.drop().select("Date", "Updated On").
      rdd.map(x => Math.abs(format.parse(x.getString(0)).getTime() - format.parse(x.getString(1)).getTime())/1000.0/60.0/60.0/24)
    time_update_case.cache()

    val newNames = Seq("x", "y", "time_to_update")
    val finalDF = df.toDF().na.drop().select("X Coordinate", "Y Coordinate").rdd.zip(time_update_case)
      .map(x => (x._1.getInt(0), x._1.getInt(1),x._2))
      .toDF(newNames: _*)

    finalDF.rdd.repartition(1).saveAsTextFile("/home/fotis/dev_projects/spark_test/target/crimes_AVG_data")

  }

}
