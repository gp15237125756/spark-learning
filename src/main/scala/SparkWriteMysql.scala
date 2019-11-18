
import java.util.Properties
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteMysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteMysql").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //下面我们设置两条数据表示两个学生信息
    val studentRDD = sc.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))
    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true)))
    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //下面创建一个prop变量用来保存JDBC连接参数
    val prop = new Properties()
    prop.put("user", "irdms") //表示用户名是root
    prop.put("password", "irdms") //表示密码是hadoop
    prop.put("driver", "com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver
    //下面就可以连接数据库，采用append模式，表示追加记录到数据库spark的student表中
    studentDataFrame.write.mode("append").jdbc("jdbc:mysql://172.18.0.104:3306/spark", "spark.student", prop)

  }
}



