package org.apache.spark.scheduler
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Created by cloud on 18/1/19.
 */
class MySparkAppListener(val sparkConf: SparkConf) extends SparkListener with Logging{

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appId = applicationStart.appId
//    logInfo(appId)
    logInfo("不知道是啥" + appId)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logInfo("app end time " + applicationEnd.time)
  }

}