
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf

  object SimpleApp {
    def main(args: Array[String]) {
      //val logFile = "file:///E:/README.md" // Should be some file on your system
      val logFile = "file:///home/hadoop/spark-2.4.3/README.md"
      //此处不加setMaster 会报错A master URL must be set in your configuration
      //参考https://stackoverflow.com/questions/38008330/spark-error-a-master-url-must-be-set-in-your-configuration-when-submitting-a
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    }
  }

