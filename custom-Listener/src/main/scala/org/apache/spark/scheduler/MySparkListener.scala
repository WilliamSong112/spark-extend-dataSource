package org.apache.spark.scheduler

/**
 * Created by Ricky on 2016/4/14 0014.
 */
class MySparkListener extends SparkListener {

  //trait JobResult
  //case class JobFailed(exception: Exception) ;
  //case object JobSucceeded extends JobResult;
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    println("*************************************************")
    println("app:end")
    println("*************************************************")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    println("*************************************************")
    println("job:end")
    jobEnd.jobResult match {
      case JobSucceeded =>
        println("job:end:JobSucceeded=-------")
      case JobFailed(exception) =>
        println("job:end:file")
        exception.printStackTrace()
    }
    println("*************************************************")
  }
}


