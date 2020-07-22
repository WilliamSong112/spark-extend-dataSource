import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//自定义 sql   data  source 主要代码在  cn.zj.spark.datasource
object APPcustomSQLdataSource {
  def main(args: Array[String]): Unit = {
    println("hello")

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    val df = spark.sqlContext.read.format("cn.zj.spark.sql.datasource").load("/Users/songyx/IdeaProjects/spark-extend-dataSource/data")

    //print the schema
    //  df.printSchema()

    //print the data
    df.show()




    //save the data
    //  df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_custom/")
    //  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_json/")
    //  df.write.mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_none/")

    //select some specific columns
    //  df.createOrReplaceTempView("test")
    //  spark.sql("select id, name, salary from test").show()

    //filter data
    //df.createOrReplaceTempView("test")
    //spark.sql("select * from test where salary = 50000").show()

    println("Application Ended...")
  }

}
