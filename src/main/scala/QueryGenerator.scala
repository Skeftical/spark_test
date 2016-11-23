import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 23/11/16.
  */
object QueryGenerator {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    //load normalized dimensions
    val df = spark.read.parquet("/home/fotis/dev_projects/spark_test/target/normalized.parquet")
    //load query ranges
    val rdd = spark.sparkContext.textFile("/home/fotis/dev_projects/spark_test/target/OUT/part-00000")
    print(rdd.first())


  }

}
