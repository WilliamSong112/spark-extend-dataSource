package custom

import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .appName("main")
      .master("local[2]")
      .config("spark.extraListeners","org.apache.spark.scheduler.MySparkAppListener")
      .getOrCreate()

    //spark.sparkContext.addSparkListener(new MySparkAppListener)
    //spark.sparkContext.addSparkListener(new MySparkAppListener: SparkListenerInterface)
    spark.stop()
  }
}
