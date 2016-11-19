import org.apache.spark.mllib.random.RandomDataGenerator

/**
  * Created by fotis on 16/11/16.
  */
class OwnGenerator extends RandomDataGenerator[Array[Double]]{

  private val random = new Multidistributions()

  override def nextValue(): Array[Double] = random.drawSample()

  override def copy(): OwnGenerator = new OwnGenerator()

  override def setSeed(seed: Long): Unit = random.setSubspaces(seed.toInt)
}
