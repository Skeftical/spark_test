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
    val sizeDataset = 50000
    //Min mean and maximum mean for each dimension
    val minMaxMeans = Array(//Means around regions of interest
      Array((9.05, 9.3),(45.35,45.50))//(x1,x2), (y1,y2) combination - region 0
//      Array((9.25, 45.50), (9.2,45.5))//region 1
    )
    val means = new Array[Array[Double]](dimensions)
    val randRegion = new Random()
    var region = 0
    for (i <- 0 until dimensions){
      region = randRegion.nextInt(1) //0 to 1
      means(i) = generate_means(minMaxMeans(region)(i)._1, minMaxMeans(region)(i)._2, subspaces)
    }
    //Broadcast variables to be available at all nodes.
    println("Means" + means.foreach(x => println(x.deep.mkString(" "))))
    val broadcastVar = sc.broadcast(means)
    val broadcastVariance = sc.broadcast(variance)
    val broadcastSub = sc.broadcast(subspaces)
    //So for subspace 0 : means is a matrix of (dXsubspaces) means correspond to Column 0
    //Generate random normal data and then transform them
//    val multiDataset = normalVectorRDD(sc, sizeDataset, dimensions, 1).map(v => {
//      val subspace = new scala.util.Random().nextInt(broadcastSub.value); //uniformly select subspace
//      for (i <- broadcastVar.value.indices)
//        yield { broadcastVar.value(i)(subspace) + broadcastVariance.value * v(i)  } //Transform Multi
//    })

    val multiDataset = uniformVectorRDD(sc, sizeDataset, dimensions, 1).map(v => {
      val subspace = new scala.util.Random().nextInt(broadcastSub.value); //uniformly select subspace
      for (i <- broadcastVar.value.indices)
        yield { broadcastVar.value(i)(subspace) + 0.04 * v(i)  } //Transform Multi (a + (b-a) * v = a + (a+1-a) * v = a + 1 * v
    })


    //Add volume for hypercube Uniform
    //Generated distributions are of the form N(0,1) for normal. Transoform to mu + sigma * x to get N(mu, sigma^2)
    val lmeans = Array(0.02, 0.04, 0.06)
//
    val uniVol = uniformRDD(sc, sizeDataset,1).map(d => {
          val a = lmeans(randRegion.nextInt(3))
          a + (a+0.02 - a) * d
        }) //Original is U(0,1) transform to U(a,b) by a + (b - a) * v)}
//    val uniVol = normalRDD(sc, sizeDataset,1).map(d => lmeans(randRegion.nextInt(3)) + 0.03 * d.abs) //mu + sigma * v
//      val mean = 0.1
//      val uniVol = exponentialRDD(sc, mean, sizeDataset,1).map(d => 0.1 + 0.09 * d)



    //Transform vector to string and save
    multiDataset.zip(uniVol).map(t => t._1.mkString(",") +"," + t._2.toString)
      .saveAsTextFile("/home/fotis/dev_projects/spark_test/target/calls_queries_uni_uni_varl-")

  }

}
