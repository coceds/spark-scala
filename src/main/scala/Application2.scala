import java.io.Serializable
import java.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Application2 {

  def main(args: Array[String]) {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "cm")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://192.168.56.101/cm"
    val table = "spark_results_2"

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
    import sqlContext.implicits._
    val customAggregator = new CustomAggregator().toColumn.as("custom")
    entries.groupBy($"productCategory", $"productName")
      .count()
      .groupBy($"productCategory")
      .agg(customAggregator)
      .as[Result]
      .flatMap(x => {
        val iterator = x.custom.iterator
        val result = ListBuffer[Tuple3[String, String, Long]]()
        while (iterator.hasNext) {
          val value = iterator.next()
          result += Tuple3(x.productCategory, value._2, value._1)
        }
        result
      })
      //.explain()
      .show(10)
    //.write.mode("append").jdbc(url, table, prop)
  }


  case class Result(productCategory: String, custom: ListBuffer[(Long, String)])

  class CustomAggregator() extends Aggregator[Row, util.PriorityQueue[Container], ListBuffer[(Long, String)]] with Serializable {

    def zero = new util.PriorityQueue[Container]()

    def reduce(acc: util.PriorityQueue[Container], x: Row) = {
      val count = x.getLong(2)
      val name = x.getString(1)
      if (acc.size() < 10) {
        acc.add(new Container(count, name))
      } else {
        val lowest = acc.peek()
        if (count > lowest.getCount) {
          acc.remove(lowest)
          acc.add(new Container(count, name))
        }
      }
      acc
    }

    def merge(acc1: util.PriorityQueue[Container], acc2: util.PriorityQueue[Container]) = {
      val iterator = acc2.iterator()
      while (iterator.hasNext) {
        val value = iterator.next()
        if (acc1.size() < 10) {
          acc1.add(value)
        } else {
          val lowest = acc1.peek()
          if (value.getCount > lowest.getCount) {
            acc1.poll()
            acc1.add(value)
          }
        }
      }
      acc1
    }

    def finish(acc: util.PriorityQueue[Container]) = {
      var result: ListBuffer[(Long, String)] = ListBuffer[(Long, String)]()
      val iterator = acc.iterator()
      while (iterator.hasNext) {
        val c = iterator.next()
        result += Tuple2(c.getCount, c.getName)
      }

      result
    }

    def bufferEncoder: Encoder[util.PriorityQueue[Container]] = Encoders.javaSerialization(classOf[util.PriorityQueue[Container]])

    def outputEncoder: Encoder[ListBuffer[(Long, String)]] = ExpressionEncoder()

  }

  class Container() extends Comparable[Container] with Serializable {
    private var count = 0L
    private var name = ""

    def this(count: Long, name: String) {
      this()
      this.count = count
      this.name = name
    }

    def getCount: Long = count

    def setCount(count: Long): Unit = {
      this.count = count
    }

    def getName: String = name

    def setName(name: String): Unit = {
      this.name = name
    }

    override def compareTo(o: Container): Int = (this.count - o.count).toInt
  }

}


