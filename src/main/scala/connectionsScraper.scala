import java.io.PrintWriter
import java.net.URL
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

import scala.io.Source

object connectionScraper {
  def main(args: Array[String]): Unit = {
    val url: String = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
    val fileName: String = "routes.csv"

    val data: String = Source.fromURL(url).getLines().mkString("\n")

    new PrintWriter(fileName) { write(data); close() }

    val spark: SparkSession = SparkSession.builder()
      .appName("connectionScraper")
      .master("local[*]")
      .getOrCreate()

    val schema: StructType = StructType(Seq(
      StructField("airline", StringType, nullable = true),
      StructField("airlineId", StringType, nullable = true),
      StructField("sourceAirport", StringType, nullable = true),
      StructField("sourceAirportId", StringType, nullable = true),
      StructField("destAirport", StringType, nullable = true),
      StructField("destAirportId", StringType, nullable = true),
      StructField("codeshare", StringType, nullable = true),
      StructField("stops", StringType, nullable = true),
      StructField("equipment", StringType, nullable = true)
    ))

    val df:DataFrame = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .schema(schema)
      .csv(fileName)

    df.show()


  }
}
