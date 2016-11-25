import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.{DenseVector, Vectors, Vector}

/**
  * Created by fotis on 25/11/16.
  */
object Regression {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder().master("local")
      .appName("Clustering method")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    import spark.implicits._
    val trainingLinear = spark.read.csv("/home/fotis/dev_projects/spark_test/target/count_query_results/part-00000").rdd
      .map(row =>{
        val label = row.getAs[String]("_c3").toDouble
        val vector = Vectors.dense(row.getAs[String]("_c0").toDouble, row.getAs[String]("_c1").toDouble, row.getAs[String]("_c2").toDouble)
        (label, vector)
      }).toDF("label","features")
    val lrModel = lr.fit(trainingLinear)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    /**
      * Add linear model by  cluster and compare.
      */

  }

}
