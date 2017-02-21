import breeze.numerics._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
/**
  * Created by fotis on 23/11/16.
  */

/**
  * TODO: Generalize to other queries apart from COUNT
  * ABSTRACT AWAY
  */
object QueryGenerator {
  //For range queries
  def isWithin(volume: Double , x : Array[Double], query : Array[Double]) :Boolean = {
    if (x.length != query.length) throw new Exception
    val tuples = query.map(d => (d-volume, d+volume)) // Transform each of the dimensions into a tuple of low and high value
    for (i <- x.indices){
      if (x(i)< tuples(i)._1 || x(i)>tuples(i)._2 ) return false
    }
    true
  }
  //For theta queries
  def isWithinEuclidean(theta : Double, x: Array[Double], query: Array[Double]) : Boolean = {
    //Euclidean distance
    val dist = sqrt(x.zip(query).map(p => p._1 - p._2).map(d => d * d).sum)
    dist<=theta
  }

  def querySetSizeSmall = true

  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Generate Query Dataset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val INPUT_DATA = "/home/fotis/dev_projects/spark_test/target/normalized.parquet"
    val QUERY_FILE = "/home/fotis/dev_projects/spark_test/target/OUT_extended_l/part-00000"
    //load normalized dimensions
    val df = spark.read.parquet(INPUT_DATA).cache()
    //load query ranges
    val rdd = spark.sparkContext.textFile(QUERY_FILE)

    val vectors: RDD[DenseVector] = df.rdd.map(
      _.getAs[DenseVector]("scaledOpen")
    )
    //Execute COUNT query over range predicates
    val queries = rdd.map(_.split(",").map(_.toDouble)).collect()

//    val inds: RDD[breeze.linalg.DenseVector[Long]] = vectors.map(v => {
//      //  Create {0, 1} indicator vector
//      breeze.linalg.DenseVector(queries.map(q => {
//        // Define as before
//        val volume = q(q.length-1)
//        val dimensions = q.slice(0, q.length-1)
//        // Output 0 or 1 for each q
//        if (isWithin(volume, v, dimensions)) 1L else 0L
//      }))
//    })
//
//    val counts: breeze.linalg.DenseVector[Long] = inds
//      .aggregate(breeze.linalg.DenseVector.zeros[Long](queries.length))(_ += _, _ += _)

    val results = queries.par.map(q => {
      val volume = q(q.length-1)
      val query = q.slice(0, q.length-1)
      val count = df.filter(row => {
        val v = row.getAs[Array[Double]]("scaledOpen")
        isWithinEuclidean(volume, v, query)
      }).count
      q.mkString(",")+","+count
    })
//    val results = queries.zip(counts.toArray).map {
//        case (q, c) => s"""${q.mkString(",")},$c"""
//    }
    val rddResults = sc.parallelize(results.toArray[String])
//    //Save File
    rddResults.saveAsTextFile("/home/fotis/dev_projects/spark_test/target/count_query_results_extended_l")
  }

}
