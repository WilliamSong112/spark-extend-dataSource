package custom

import org.apache.spark.scheduler.MySparkListener
import org.apache.spark.{SparkConf, SparkContext}


object JobProcesser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaWordCountProducer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    /*  sc.setJobGroup("test1","testdesc")
      val completedJobs= sc.jobProgressListener*/
    sc.addSparkListener(new MySparkListener)
    val rdd1 = sc.parallelize(List(('a', 'c', 1), ('b', 'a', 1), ('b', 'd', 8)))
    val rdd2 = sc.parallelize(List(('a', 'c', 2), ('b', 'c', 5), ('b', 'd', 6)))
    val rdd3 = rdd1.union(rdd2).map {
      x => {
        Thread.sleep(500)
        x
      }
    }.count()
    //此处故意出错  测试 自己的  监听器
//    rdd1.map(x => 0.2).map(x => 0).map {
//      x => {
//        if (x == 0) {
//          throw new Exception("my exeception")
//        }
//      }
//        x
//    }.reduce(_ + _)
    println(rdd3)
    sc.stop()
  }

}
