import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.util.matching.Regex

case class LogData(bytes: Int, number: Int)

object SimpleApp {
  def main(args: Array[String]) {
    println("Start.")

    val pattern = "^([^\\s]+) .+ (\\d+) .+$".r
    val logFile = "/user/root/spark_hw1/access_logs_test.log" // Should be some file on your system


    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val logLines = sc.textFile(logFile)

    val logDatas = logLines.map((line: String) => {
      println("line: " + line)

      val pattern(t1, t2) = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
      println("t1 " + t1 + " t2 " + t2)


      val pattern(ip, bytes) = line
//      new LogData(ip, bytes.toInt)
//      (ip, new LogData(bytes.toInt, 1))
      ("ip1", new LogData(10, 1))
    })
    .reduceByKey((data: LogData, data0: LogData) => new LogData(data.bytes + data0.bytes, data.number + data0.number))
    .foreach((tuple: (String, LogData)) => {
      println(tuple._1 + "," + tuple._2.bytes / tuple._2.number + "," + tuple._2.bytes)
    })
    
    





//    println("Lines with l: %s, Lines with i: %s".format(numAs, numBs))
  }
}