import IDEASdb.FML
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 29/11/16.
  */
object Pipeline {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Clustering method")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //Training Data
    import spark.implicits._
    val trainingData = spark.read.csv("/home/fotis/dev_projects/spark_test/target/count_query_results/part-00000").rdd
      .map(row =>{
        val label = row.getAs[String]("_c3").toLong
        val vector = Vectors.dense(row.getAs[String]("_c0").toDouble, row.getAs[String]("_c1").toDouble, row.getAs[String]("_c2").toDouble)
        (label, vector)
      }).toDF("label","features")

    println(trainingData.show())

    val model = new FML()

    val m = model.fit(trainingData)
    val d = m.transform(trainingData)
    println(d.show())
//    val kmeans = new KMeans()
//      .setK(5)
//      .setSeed(1L)
//      .setFeaturesCol("features")
//      .setPredictionCol("prediction")
//
//    val model = kmeans.fit(trainingData)
//    val data = model.transform(trainingData)
////    data.select($"features").where("prediction = 3")
//
//    //Multiple LR models
//
//    val lrModels = for (i <- 0 until model.getK) yield {
//      i -> new LinearRegression()
//        .setMaxIter(10)
//        .setRegParam(0.3)
//        .setElasticNetParam(0.8)
//    }
//    val trainedModels = lrModels.par.map(seq => seq._2.fit(data.select($"features",$"label").where("prediction = "+seq._1)))
//
//    trainedModels.foreach(lrModel => {
//      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//      println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
//      println(s"r2: ${lrModel.summary.r2}")
//    })



  }

}
