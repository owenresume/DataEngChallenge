import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf


object DataEngChallenge
{
  def main(args:Array[String]) : Unit =
  {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.json("data/location-data-sample")

    println("Number of location events per IDFA:")
    df.groupBy("idfa").count().agg(max("count"), min("count"), avg("count"), stddev("count")).show()
    /*
    +----------+----------+----------------+------------------+
    |max(count)|min(count)|      avg(count)|stddev_samp(count)|
    +----------+----------+----------------+------------------+
    |     15979|         1|36.7517578953113|118.61139276213811|
    +----------+----------+----------------+------------------+*/

    // select all distinct (geohash,lat,lng) triplets
    val geohashes = df.select("geohash", "lat", "lng").distinct()

    // For each distinct (geohash,idfa) pair, calculate the 7-digit geohash prefix in a new column
    // Also, filter out null locations
    val prefix7 = udf[String,String] (_.substring(0,7))
    val prefix7DF = df.select("geohash", "idfa").filter($"geohash" =!= "s00000000000").distinct().withColumn("prefix", prefix7(df("geohash")))
    prefix7DF.cache()

    println("The most common 7-digit geohash prefixes, with count of their distinct idfa's:")
    // Find the prefixes with the most distinct idfa's
    val mostCommon7Prefix = prefix7DF.select("idfa", "prefix").distinct().groupBy("prefix").count().orderBy(desc("count"))
    // Write this finding to parquet format as asked in the instructions #6
    mostCommon7Prefix.write.mode(SaveMode.Overwrite).parquet("data/most-common-7-prefix")
    mostCommon7Prefix.show(10)
    /*
    +-------+-----+
    | prefix|count|
    +-------+-----+
    |dpz8336|  131|
    |dpz83ek|  103|
    |f25dyjf|   76|
    |djfq0rz|   75|
    |dq21mme|   73|
    |f25dv90|   69|
    |f25dtxu|   64|
    |9vkh7wd|   63|
    |f244mdx|   62|
    |dpz83dt|   61|
    +-------+-----+*/

    // Most common prefix is: dpz8336 (131 people)
    prefix7DF.filter($"prefix" === "dpz8336").show(1)
    /*
    +------------+--------------------+-------+
    |     geohash|                idfa| prefix|
    +------------+--------------------+-------+
    |dpz8336uu2eq|20c5840e-e1de-40a...|dpz8336|
    +------------+--------------------+-------+*/

    geohashes.filter($"geohash" === "dpz8336uu2eq").show()
    /*
    +------------+---------+-----------+
    |     geohash|      lat|        lng|
    +------------+---------+-----------+
    |dpz8336uu2eq|43.645381|-79.3942298|
    +------------+---------+-----------+*/

    println("Largest cluster of different people is 131, within ~100m of (43.645381,-79.3942298)")
    // Searching (43.645381,-79.3942298) on Google Maps shows
    // it is the Freckle IOT office!
    println("From Google Maps, it appears to be the FreckleIOT office: https://www.google.com/maps/place/43%C2%B038'43.4%22N+79%C2%B023'39.2%22W/@43.645381,-79.394777,19z/data=!4m5!3m4!1s0x0:0x0!8m2!3d43.645381!4d-79.3942298")


    // Second most common prefix is: dpz83ek (103 people)
    prefix7DF.filter($"prefix" === "dpz83ek").show(1)
    /*
    +------------+--------------------+-------+
    |     geohash|                idfa| prefix|
    +------------+--------------------+-------+
    |dpz83eks8w2f|6afc0074-76dd-48d...|dpz83ek|
    +------------+--------------------+-------+*/

    geohashes.filter($"geohash" === "dpz83eks8w2f").show()
    /*
    +------------+----------+-----------+
    |     geohash|       lat|        lng|
    +------------+----------+-----------+
    |dpz83eks8w2f|43.6563545|-79.3810045|
    +------------+----------+-----------+*/

    println("Second largest cluster of different people is 103, within ~100m of (43.6563545,-79.3810045)")
    // Searching (43.6563545,-79.3810045) on Google Maps shows
    // it is Yonge & Dundas in Toronto.
    println("From Google Maps, it appears to be Yonge & Dundas in Toronto: https://www.google.com/maps/place/43%C2%B039'22.9%22N+79%C2%B022'51.6%22W/@43.6563545,-79.3831932,17z/data=!4m5!3m4!1s0x0:0x0!8m2!3d43.6563545!4d-79.3810045")
  }
}
