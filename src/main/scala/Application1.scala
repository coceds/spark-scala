import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}

object Application1 {

  def main(args: Array[String]) {

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "cm")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://192.168.56.101/cm"
    val table = "spark_results_1"

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Big Data")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val source = sqlContext.read.text("hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/06/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/07/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/08/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/09/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/10/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/11/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/12/")
      .as[String]

    val entries = source.map(line => {
      val parts = line.split(",")
      Entry(parts(0), parts(1), parts(2), parts(3).toDouble, parts(4), parts(5))
    })

    entries.groupBy("productCategory")
      .count().as("count")
      //.toDF()
      .sort(functions.desc("count"))
      .limit(10)
      //.write.mode("append").jdbc(url, table, prop)
      .show(10)
  }

}

case class Entry(purchaseDate: String, identifier: String, productName: String,
                 productPrice: Double, productCategory: String, ip: String)

