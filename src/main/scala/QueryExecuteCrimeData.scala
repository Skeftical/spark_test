import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 23/02/17.
  */
object QueryExecuteCrimeData {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Generate Query Dataset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


//    val df = spark.read.option("header","true").csv("/home/fotis/DATA/Crimes_-_2001_to_present.csv")
//    val sample = df.select("Case Number","X Coordinate","Y Coordinate")
//    val newNames = Seq("case_number", "x", "y")
//    val dfRenamed = sample.toDF(newNames: _*)
//
//    val featureDF = dfRenamed.where("y is not null AND x <> 0 AND y<>0")
//      .rdd.map(row => (row.getString(0),
//      Vectors.dense(row.getString(1).toInt, row.getString(2).toInt))).toDF("label","features")
//
//    featureDF.persist()
//
//    val scaler = new MinMaxScaler()
//      .setInputCol("features")
//      .setOutputCol("x_scaled")
//      .setMax(0.5)
//      .setMin(-0.5)
//    val scalerModel = scaler.fit(featureDF)
//    val scaledData = scalerModel.transform(featureDF)
//
//
//    val newDF = scaledData.select("x_scaled").rdd.map(row => {
//      val v = row.getAs[DenseVector]("x_scaled")
//      (v(0),v(1))
//    }).toDF("x","y")

    /**
      * If data already created and normalized
      */
    val ddf = spark.read.option("inferSchema", "true").csv("/home/fotis/dev_projects/spark_test/target/crimeDataCoordinates_normalized_full")
    val newNames = Seq("x","y")
    val dfRenamed = ddf.toDF(newNames: _*)


    dfRenamed.persist().createOrReplaceTempView("points")

    val QUERY_FILE = "/home/fotis/dev_projects/spark_test/target/OUT_high_variance_l_norm_-05to05/part-00000"
    val rdd = spark.sparkContext.textFile(QUERY_FILE)

    val queries = rdd.map(_.split(",").map(_.toDouble)).collect() //if query number too large then this can cause problems

    val results = queries.par.map(q => {
      val theta = q(2)
      val x1 = q(0)
      val x2 = q(1)
      val count = spark.sql(s"SELECT * FROM points WHERE $theta > sqrt(power($x1 - x, 2) + power($x2 - y, 2))").count()
      q.mkString(",")+","+count
    })

    val rddResults = sc.parallelize(results.toArray[String])
    //    //Save File
    rddResults.saveAsTextFile("/home/fotis/dev_projects/spark_test/target/count_query_results_HIGH-VAR-THETA_NORM-THETA_CRIME-DS_DATSPACES-5")
  }
}
