/**
  * Created by fotis on 16/11/16.
  */
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.sql.SparkSession
import scala.util.Random

/**
  * Class that generates hyper cubes over range predicates to facilitate the execution of
  * aggregate queries.
  */


object DataGeneration {
  private def generate_means(max : Double, min : Double, subspaces: Int) : Array[Double] = {
    val rand = new Random()
    val means = for (i <- 1 to subspaces) yield {
      min + (max-min) * rand.nextDouble()
    }
    means.toArray
  }
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Data Generation")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext
    /**
      * Initialize parameters and means for subspaces
      */
    val subspaces  = 5
    val dimensions = 2
    val variance = 0.01
    val sizeDataset = 10000
    //Min mean and maximum mean for each dimension
    val minMaxMeans = Array(//Means around regions of interest
      (0.05, 0.2),
      (0.10, 0.25)
    )
    val means = new Array[Array[Double]](dimensions)

    for (i <- 0 until dimensions){
      means(i) = generate_means(minMaxMeans(i)._1, minMaxMeans(i)._2, subspaces)
    }
    //Broadcast variables to be available at all nodes.
    val broadcastVar = sc.broadcast(means)
    val broadcastVariance = sc.broadcast(variance)
    val broadcastSub = sc.broadcast(subspaces)
    //So for subspace 0 : means is a matrix of (dXsubspaces) means correspond to Column 0
    //Generate random normal data and then transform them
    val multiDataset = normalVectorRDD(sc, sizeDataset, dimensions, 1).map(v => {
      val subspace = new scala.util.Random().nextInt(broadcastSub.value); //uniformly select subspace
      for (i <- broadcastVar.value.indices)
        yield { broadcastVar.value(i)(subspace) + broadcastVariance.value * v(i)  } //Transform Multi
    })
    //Add volume for hypercube
    val uniVol = uniformRDD(sc, sizeDataset,1).map(d => 0.1 + 0.01 * d)


    //Transform vector to string and save
    multiDataset.zip(uniVol).map(t => t._1.mkString(",") +"," + t._2.toString)
      .saveAsTextFile("/home/fotis/dev_projects/spark_test/target/OUT")

  }

}
