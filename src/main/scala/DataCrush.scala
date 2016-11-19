import java.text.{SimpleDateFormat, DateFormat}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.sql.types.{DateType, StringType, StructType, StructField}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}


/**
  * Created by fotis on 15/10/16.
  */

object DataCrush {
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

      val df = new SimpleDateFormat("MM/dd/yyyy H:m")
      var sampleDate = df.parse("09/27/2013 9:00")
      val t1 =  sampleDate.getTime % (24*60*60*1000L)
      sampleDate = df.parse("09/27/2013 10:00")
      val t2 = sampleDate.getTime % (24*60*60*1000L)
      val txt = file.rdd.map(row => Row(row.getAs[String]("Primary Type"),
        row.getAs[String]("Date"),
        row.getAs[String]("Beat"),
        row.getAs[String]("Latitude"),
        row.getAs[String]("Longitude"))).filter(row => Math.abs((df.parse(row.getAs[String](1)).getTime % (24*60*60*1000L))-t1) < 540000
        && ( row.getString(0).compareTo("ASSAULT") == 0
        || row.getString(0).compareTo("BATTERY") == 0
        || row.getString(0).compareTo("HOMICIDE") == 0
        || row.getString(0).compareTo("THEFT") == 0 )
      )


      // The schema is encoded in a string
      val schemaString = "primary_type date beat latitude longitude"

      // Generate the schema based on the string of schema
      val fields = schemaString.split(" ")
      val structFields : Array[StructField] = new Array[StructField](5)
      structFields(0) = StructField(fields(0), StringType, nullable = true)
      structFields(1) = StructField(fields(1), StringType, nullable = true)
      structFields(2) = StructField(fields(2), StringType, nullable = true)
      structFields(3) = StructField(fields(3), StringType, nullable = true)
      structFields(4) = StructField(fields(4), StringType, nullable = true)



      val schema = StructType(structFields)

      val dff = spark.createDataFrame(txt, schema)
      dff.write.mode("append").json("/home/fotis/dev_projects/hackathon2016/crimeFiltered")
    }
  }

