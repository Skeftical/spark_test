/**
  * Created by fotis on 02/09/16.
  */

import java.text.SimpleDateFormat

import SimpleClass._
import breeze.numerics.sqrt
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

object Secondary {

  def distance(a: Array[Double], b: Array[Double]) =
    sqrt(a.zip(b).map(p => p._1 - p._2).map(d => d * d).sum)

  def clusteringScore(data : RDD[org.apache.spark.mllib.linalg.Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    val centroids = model.clusterCenters
    data.map(datum => distance(centroids(model.predict(datum)).toArray, datum.toArray )).mean
  }

  def main(args : Array[String]): Unit ={
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val log = Logger.getRootLogger
    log.setLevel(Level.DEBUG)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val file = spark.read
      .option("header", "true")
      .csv("/home/fotis/dev_projects/hackathon2016/ChicagoCrimeData_sample.csv")


    val dataAndLabel = file.rdd.filter(row => {
      !row.getAs[String]("Beat").isEmpty && !row.getAs[String]("District").isEmpty
    }).map( row => {
        val label = row.getAs[String]("Primary Type")
        val vector = Vectors.dense(row.getAs[String]("Beat").toDouble , row.getAs[String]("District").toDouble)
        (vector, label)
      })

    val data = dataAndLabel.map(_._1).cache()


//    val kmeans = new KMeans()
//    val model = kmeans.run(data)
//
//    model.clusterCenters.foreach(centroid => {
//      log.warn(centroid.toString)
//    })
//
//    val clusterAndLabel = dataAndLabel.map {case
//      (vector, label) => (model.predict(vector), label)
//    }
//
//    val clusterLabelCount = clusterAndLabel.countByValue
//
//    clusterLabelCount.toList.sorted.foreach {case
//      ((cluster,label),count) => log.warn(f"$cluster%1s$label%18s$count%8s")
//    }

    data.unpersist(true)

    val numCols = data.first().size
    val n = data.count()
    val sums = data.reduce((a,b) =>
      Vectors.dense(a.toArray.zip(b.toArray).map(t => t._1 + t._2))
    )

    val sumSquares = data.fold(Vectors.dense(new Array[Double](numCols))){
      (a,b) => Vectors.dense(a.toArray.zip(b.toArray).map(t => t._1 + t._2 * t._2))
    }

    val stDevs = sumSquares.toArray.zip(sums.toArray).map {
      case (sumSq, sum) => sqrt(n*sumSq - sum*sum)/n
    }

    val means = sums.toArray.map(_ / n)

    val normalizedData = data.map( t=>
      (t.toArray,means,stDevs).zipped.map((value, mean, stdev) =>
        if (stdev <= 0) (value-mean) else
          (value-mean)/stdev)).cache()

    val kScores = (30 to 100 by 10).par.map(k =>
      (k, clusteringScore(data, k))).foreach(r => log.warn(r))








  }
}
