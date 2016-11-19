import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.MultivariateGaussian
import java.util.Random
/**
  * Created by fotis on 16/11/16.
  */
class Multidistributions extends Serializable {

  private var subspaces = 5
  val maxMean1 = 0.2
  val minMean1 = 0.05
  val maxMean2 = 0.25
  val minMean2 = 0.10
  val variance = 0.01
  val rand =  new Random()

  val cov = new DenseMatrix(2, 2, Array(variance,0.0,0.0,variance))

  val multivariates = for (i <- 1 to subspaces) yield {
    val mu = DenseVector(Array(minMean1 + (maxMean1-minMean1) * rand.nextDouble(),
      minMean2 + (maxMean2-minMean2)*rand.nextDouble()))
    MultivariateGaussian(mu, cov)
  }
  def setSubspaces(s : Int) = subspaces = s

  def selectSubspace() : MultivariateGaussian = multivariates(rand.nextInt(subspaces))

  def drawSample() : Array[Double] = selectSubspace().draw().data

}
