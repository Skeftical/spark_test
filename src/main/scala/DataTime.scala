import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by fotis on 15/10/16.
  */
object DataTime {
  def main(args: Array[String]): Unit = {
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
      .csv("/home/fotis/dev_projects/hackathon2016/ChicagoCrimeDataAll.csv")

    val df = new SimpleDateFormat("MM/dd/yyyy")
    var sampleDate = df.parse("09/11/2011")
    val t1 =  sampleDate.getTime
    val cal = Calendar.getInstance()

//    val txt = file.rdd.map(row => Row(row.getAs[String]("Primary Type"),
//      row.getAs[String]("Date"),
//      row.getAs[String]("Beat"),
//      row.getAs[String]("Latitude"),
//      row.getAs[String]("Longitude"))).filter(row => Math.abs(df.parse(row.getAs[String](1)).getTime-t1) < 2628000000L
//      && ( row.getString(0).compareTo("ASSAULT") == 0
//      || row.getString(0).compareTo("BATTERY") == 0
//      || row.getString(0).compareTo("HOMICIDE") == 0
//      || row.getString(0).compareTo("THEFT") == 0 )
//    )
    val txt = file.rdd.map(row => Row(row.getAs[String]("Primary Type"),
      row.getAs[String]("Date"),
      row.getAs[String]("Beat"),
      row.getAs[String]("Latitude"),
      row.getAs[String]("Longitude")))
      .filter(row => Math.abs(df.parse(row.getAs[String](1)).getTime-t1) < 2628000000L).count()
//      .map(row => {
////            cal.setTime(df.parse(row.getAs[String]("Date")))
////            val year = cal.get(Calendar.YEAR)
////            val month = cal.get(Calendar.MONTH)
////            val day = cal.get(Calendar.DAY_OF_MONTH)
////            val date = "%d-%d-%d".format(year,month,day)
////            Row(row.getAs[String]("Primary Type"), date)
//              if (df.parse(row.getAs[String](1)).before(sampleDate)){
//                  (0,row.getAs[String](0))
//              }else {
//                (1,row.getAs[String](0))
//              }
//          }).count()
    log.warn(txt)


    // The schema is encoded in a string
//    val schemaString = "primary_type date"
//
//    // Generate the schema based on the string of schema
//    val fields = schemaString.split(" ")
//    val structFields : Array[StructField] = new Array[StructField](5)
//    structFields(0) = StructField(fields(0), StringType, nullable = true)
//    structFields(1) = StructField(fields(1), StringType, nullable = true)
//    structFields(2) = StructField(fields(2), StringType, nullable = true)
//    structFields(3) = StructField(fields(3), StringType, nullable = true)
//    structFields(4) = StructField(fields(4), StringType, nullable = true)



//    val schema = StructType(structFields)
//
//    val dff = spark.createDataFrame(txt,schema)
//    log.warn(dff.groupBy("date").count())
//    dff.write.mode("append").json("/home/fotis/dev_projects/hackathon2016/09-11")



  }
}
