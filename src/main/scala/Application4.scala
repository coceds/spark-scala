import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}

object Application4 {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Big Data")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val source = sqlContext.read.text("hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/06/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/07/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/08/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/09/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/10/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/11/", "hdfs://192.168.56.101:8020/user/cloudera/flume/events/2018/01/12/")
      .as[String]
      .map(line => {
        val parts = line.split(",")
        (parts(2), parts(4))
      })
      .toDF("productName", "productCategory")

    val spec = Window.partitionBy("productCategory").orderBy(functions.desc("count"))

    val res = source.groupBy("productCategory", "productName")
      .count().as("count")
      .withColumn("rowNumber", functions.row_number().over(spec))
      .where(functions.col("rowNumber") <= 10)
      .select("productCategory", "productName", "count")


    System.out.println(res.count())
    //.printSchema()
  }
}


