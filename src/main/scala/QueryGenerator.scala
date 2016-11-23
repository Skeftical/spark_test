import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 23/11/16.
  */

/**
  * TODO: Generalize to other queries apart from COUNT
  * ABSTRACT AWAY
  */
object QueryGenerator {
  def isWithin(volume: Double , x : DenseVector, dimensions : Array[Double]) :Boolean = {
    if (x.size != dimensions.length) throw new Exception
    val tuples = dimensions.map(d => (d-volume, d+volume)) // Transform each of the dimensions into a tuple of low and high value
    for (i <- 0 until x.size){
      if (x(i)< tuples(i)._1 || x(i)>tuples(i)._2 ) return false
    }
    true
  }

  def querySetSizeSmall = true

  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //load normalized dimensions
    val df = spark.read.parquet("/home/fotis/dev_projects/spark_test/target/normalized.parquet").cache()
    //load query ranges
    val rdd = spark.sparkContext.textFile("/home/fotis/dev_projects/spark_test/target/OUT/part-00000")

    //Execute COUNT query over range predicates
    //If small number of queries collect and broadcast to every partition
    val queries = rdd.map(_.split(",").map(_.toDouble)).collect()

//    val results = queries.par.map(q => {
//      val volume = q(q.length-1)
//      val dimensions = q.slice(0, q.length-1)
//      val count = df.filter(row => {
//        val v = row.getAs[DenseVector]("scaledOpen")
//        isWithin(volume, v, dimensions)
//      }).persist().count()
//      q.mkString(",")+","+count
//    })
    if (querySetSizeSmall){
      val broadQueries = sc.broadcast(queries)

    }
    val rddResults = sc.parallelize(results.toArray[String])
    //Save File
    rddResults.saveAsTextFile("/home/fotis/dev_projects/spark_test/target/count_query_results")
  }

}
