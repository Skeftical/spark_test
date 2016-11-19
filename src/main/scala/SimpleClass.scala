import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.reflect.ClassTag

/**
  * Created by fotis on 02/09/16.
  */
object SimpleClass {
  implicit class NewRDD[T: ClassTag](rdd : RDD[T]){
    def a = 1
  }


}
