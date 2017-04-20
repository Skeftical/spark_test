package IDEASdb

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.functions._

import scala.collection.parallel.ParSeq


/**
  * Created by fotis on 20/04/17.
  */

private[IDEASdb] trait FMLParams extends Params {
  val predictionCol : Param[String] = new Param[String](this, "preddefault", "prediction column name")
  setDefault(predictionCol, "prediction")
  val featuresCol : Param[String] = new Param[String](this, "featDefault", "feature column name")
  setDefault(featuresCol, "features")
}


class FMLModel (override val uid: String, private val unsupervisedModel: KMeansModel,
                private val supervisedModel: ParSeq[(Int, LinearRegressionModel)])
  extends Model[FMLModel]  with FMLParams{
  override def copy(extra: ParamMap): FMLModel = {
    val copied = new FMLModel(uid, unsupervisedModel, supervisedModel)
    copyValues(copied, extra)
  }
  /**
    * Equivalent to predict method
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val ds = this.unsupervisedModel.transform(dataset)
    val dfs = supervisedModel.par.map(m => m._2.transform(ds.select("features").where("prediction = "+m._1)))
    val df = dfs.reduce((df1,df2) => df1.union(df2))
    df
//    val predictUDF = udf((vector: Vector) => predict(vector))
//    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

//  def predict(features: Vector): Int = parentModel.get

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

}


class FML (override val uid: String) extends Estimator[FMLModel] {

  def this() = this(Identifiable.randomUID("fml"))
  override def fit(dataset: Dataset[_]): FMLModel = {
    val kmeans = new KMeans()
      .setK(5)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val parentModel = kmeans.fit(dataset)
    val tdataset = parentModel.transform(dataset)
    val lrModels = for (i <- 0 until parentModel.getK) yield {
      i -> new LinearRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
    }
    val trainedModels = lrModels.par.map(seq => (seq._1, seq._2.fit(tdataset.toDF().select("features","label").where("prediction = "+seq._1))))
    val model = copyValues(new FMLModel(uid, parentModel, trainedModels))
    model
  }

  override def copy(extra: ParamMap): FML = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

}
