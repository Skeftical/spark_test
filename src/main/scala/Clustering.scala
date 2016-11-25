import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
/**
  * Created by fotis on 25/11/16.
  * Needs more optimization more generic
  */
object Clustering {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Clustering method")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val trainingData = spark.read.csv("/home/fotis/dev_projects/spark_test/target/count_query_results/part-00000").rdd
      .map(row =>{
        val label = row.getAs[String]("_c3").toLong
        val vector = Vectors.dense(row.getAs[String]("_c0").toDouble, row.getAs[String]("_c1").toDouble, row.getAs[String]("_c2").toDouble)
        (label, vector)
      }).toDF("label","features")


    val kmeans = new KMeans().setK(5).setSeed(1L)
    val model = kmeans.fit(trainingData)

    val WSSSE = model.computeCost(trainingData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")


    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    println(model.explainParams())
    println(model.getPredictionCol)

  }

}
