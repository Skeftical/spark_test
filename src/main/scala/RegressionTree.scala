import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 08/12/16.
  */
object RegressionTree {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Clustering method")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


  }

  }
