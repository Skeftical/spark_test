import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.mllib.evaluation.RegressionMetrics

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
      .setFitIntercept(false)

    import spark.implicits._
    val trainingLinear = spark.read.csv("/home/fotis/dev_projects/spark_test/target/count_query_results/part-00000").rdd
      .map(row =>{
        val label = row.getAs[String]("_c3").toDouble
        val vector = Vectors.dense(row.getAs[String]("_c0").toDouble, row.getAs[String]("_c1").toDouble, row.getAs[String]("_c2").toDouble)
        (label, vector)
      }).toDF("label","features")

    val Array(training, test) = trainingLinear.randomSplit(Array(0.9, 0.1), seed = 12345)

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEstimatorParamMaps(Array(lr.extractParamMap()))
      .setEvaluator(new RegressionEvaluator)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)
    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    val ds = model.transform(test)

    val metrics = new RegressionMetrics(ds.select("label", "prediction").rdd.map(row =>(row.getAs[Double]("prediction"),
      row.getAs[Double]("label"))))

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")

//    val lrModel = lr.fit(trainingLinear)
//
//    // Print the coefficients and intercept for linear regression
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//
//    // Summarize the model over the training set and print out some metrics
//    val trainingSummary = lrModel.summary
//    println(s"numIterations: ${trainingSummary.totalIterations}")
//    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
//    trainingSummary.residuals.show()
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"r2: ${trainingSummary.r2}")

    /**
      * Add linear model by  cluster and compare.
      */

  }

}
