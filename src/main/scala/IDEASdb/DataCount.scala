package IDEASdb

import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 20/04/17.
  */
object DataCount {

  def main(args : Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val ddf = spark.read.option("inferSchema", "true").csv("/home/fotis/dev_projects/spark_test/target/crimeDataCoordinates_normalized_full")
    val newNames = Seq("x","y")
    val dfRenamed = ddf.toDF(newNames: _*)

    val theta = 0.01
    val x1 = 0.2
    val x2 = -0.2
    dfRenamed.persist().createOrReplaceTempView("points")

    val count = spark.sql(s"SELECT * FROM points WHERE $theta > sqrt(power($x1 - x, 2) + power($x2 - y, 2))").count()
    println("Count is " + count)
  }

}
