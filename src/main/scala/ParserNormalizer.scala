import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
/**
  * Created by fotis on 22/11/16.
  */

object ParserNormalizer {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    val file = spark.read.option("header","true").csv("/home/fotis/DATA/stock_data.csv")
    val df = file.select($"open",$"close",$"high").filter(row => !(row.getString(0).isEmpty || row.getString(1).isEmpty
      || row.getString(2).isEmpty)).rdd.map(row => (row.getString(2).toDouble ,Vectors.dense(
          row.getString(0).toDouble, row.getString(1).toDouble))).toDF("label","features")


    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledOpen")
      .setMax(0.5)
      .setMin(-0.5)
    val scalerModel = scaler.fit(df)
    val scaledData = scalerModel.transform(df)
  }
}