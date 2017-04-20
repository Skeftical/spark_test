package IDEASdb

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.functions._


/**
  * Created by fotis on 20/04/17.
  */

private[IDEASdb] trait FMLParams extends Params {
  val predictionCol : Param[String] = new Param[String](this, "preddefault", "prediction column name")
  setDefault(predictionCol, "prediction")
  val featuresCol : Param[String] = new Param[String](this, "featDefault", "feature column name")
  setDefault(featuresCol, "features")
}


class FMLModel (override val uid: String)
  extends Model[FMLModel]  with FMLParams{
  override def copy(extra: ParamMap): FMLModel = {
    val copied = new FMLModel(uid)
    copyValues(copied, extra)
  }
  /**
    * Equivalent to predict method
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  def predict(features: Vector): Int = 1

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

}


class FML (override val uid: String) extends Estimator[FMLModel] {

  def this() = this(Identifiable.randomUID("fml"))
  override def fit(dataset: Dataset[_]): FMLModel = new FMLModel(uid)

  override def copy(extra: ParamMap): FML = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

}
