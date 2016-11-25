import org.apache.spark.ml.linalg.{DenseVector, Vectors, Vector}
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
      .appName("Generate Query Dataset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //load normalized dimensions
    val df = spark.read.parquet("/home/fotis/dev_projects/spark_test/target/normalized.parquet").cache()
    //load query ranges
    val rdd = spark.sparkContext.textFile("/home/fotis/dev_projects/spark_test/target/OUT/part-00000")

    val vectors: RDD[DenseVector] = df.rdd.map(
      _.getAs[DenseVector]("scaledOpen")
    )
    //Execute COUNT query over range predicates
    val queries = rdd.map(_.split(",").map(_.toDouble)).collect()

    val inds: RDD[breeze.linalg.DenseVector[Long]] = vectors.map(v => {
      //  Create {0, 1} indicator vector
      breeze.linalg.DenseVector(queries.map(q => {
        // Define as before
        val volume = q(q.length-1)
        val dimensions = q.slice(0, q.length-1)
        // Output 0 or 1 for each q
        if (isWithin(volume, v, dimensions)) 1L else 0L
      }))
    })

    val counts: breeze.linalg.DenseVector[Long] = inds
      .aggregate(breeze.linalg.DenseVector.zeros[Long](queries.length))(_ += _, _ += _)

//    val results = queries.par.map(q => {
//      val volume = q(q.length-1)
//      val dimensions = q.slice(0, q.length-1)
//      val count = df.filter(row => {
//        val v = row.getAs[DenseVector]("scaledOpen")
//        isWithin(volume, v, dimensions)
//      }).count
//      q.mkString(",")+","+count
//    })
    val results = queries.zip(counts.toArray).map {
        case (q, c) => s"""${q.mkString(",")},$c"""
    }
    val rddResults = sc.parallelize(results)
//    //Save File
    rddResults.saveAsTextFile("/home/fotis/dev_projects/spark_test/target/count_query_results")
  }

}
