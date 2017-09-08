import org.apache.spark.sql.SparkSession

/**
  * Created by fotis on 29/08/17.
  */
object QueryExecuteCallsData {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Generate Query Dataset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = spark.read.option("sep", "\t").option("inferSchema", "true").csv("/home/fotis/DATA/calls_data/sms-call-internet-mi-2013-11-01.txt")
    val newNames = Seq("square_id", "time_interval", "country_code", "smsin", "smsout", "callin", "callout", "internet_traffic")
    val dfRenamed = df.toDF(newNames: _*)
    val milano_grid = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/fotis/DATA/calls_data/coordinates.csv")
    milano_grid.cache()
    dfRenamed.cache()
    dfRenamed.createOrReplaceTempView("calls")

    milano_grid.createOrReplaceTempView("grid")
    val summary = spark.sql("SELECT AVG(smsin) as avg_smsin, square_id FROM calls GROUP BY square_id")
    summary.persist()

    var queries = spark.read.option("inferSchema", "true").csv("/home/fotis/dev_projects/spark_test/target/calls_queries_gau_gau_varl-0.0009_varx-0.0001/part-00000").collect()
//    queries = queries.slice(1,5)
    // queries.registerTempTable("queries")
    val results = queries.par.map(q => {
      val theta = q.getDouble(2)
      val x1 = q.getDouble(0)
      val x2 = q.getDouble(1)
      val squareIds = spark.sql(s"SELECT cellId FROM grid WHERE $theta > sqrt(power($x1 - x, 2) + power($x2 - y, 2)) GROUP BY cellId") //Euclidean distance query
      val result = summary.join(squareIds, summary("square_id") === squareIds("cellId")).agg(Map("avg_smsin" -> "avg"))
      q.mkString(",")+","+result.head()(0)
    })

    val rddResults = sc.parallelize(results.toArray[String])
    rddResults.repartition(1).saveAsTextFile("/home/fotis/dev_projects/spark_test/target/calls_AVG_results")
  }
}
