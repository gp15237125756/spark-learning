
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object SparkWriteHive {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHive").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hiveCtx = new HiveContext(sc)
    val studentRDD = sc.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))
    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true)))
    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDataFrame = hiveCtx.createDataFrame(rowRDD, schema)
    //下面注册临时表
    studentDataFrame.registerTempTable("tempTable")
    //把临时表中的数据插入到Hive数据库中的sparktest.student表
    hiveCtx.sql("insert into sparktest.student select * from tempTable")
  }
}



