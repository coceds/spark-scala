import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}

object Application3 {

  def main(args: Array[String]): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "cm")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://192.168.56.101/cm"
    val table = "spark_results_3"

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Big Data")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val geodata = sqlContext.read.text("hdfs://192.168.56.101:8020/user/cloudera/geodata")
      .as[String]
      .map(line => {
        val parts = line.split(",")
        GeoData(parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
      })

    val countries = sqlContext.read.text("hdfs://192.168.56.101:8020/user/cloudera/countries")
      .as[String]
      .map(line => {
        line.split(",")
      })
      .filter(t => t.length >= 6)
      .map(parts => {
        Country(parts(0), parts(5))
      })

    val source = sqlContext.read.text("hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/06/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/07/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/08/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/09/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/10/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/11/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/12/")
      .as[String]

    val entries = source.map(line => {
      val parts = line.split(",")
      Entry(parts(0), parts(1), parts(2), parts(3).toDouble, parts(4), parts(5))
    })

    entries
      .join(functions.broadcast(geodata), Seq("ip"), "left_outer")
      .groupBy($"id")
      .sum("productPrice")
      .sort(functions.desc("sum(productPrice)"))
      .limit(10)
      .join(functions.broadcast(countries), Seq("id"), "left_outer")
      .as[Tuple3[String, Double, String]]
      .show(10)
    //.write.mode("append").jdbc(url, table, prop)
  }

}

case class Country(id: String, countryName: String)

case class GeoData(ip: String, id: String, registeredId: String,
                   representedId: String, isAnonymous: String, isProvider: String)
