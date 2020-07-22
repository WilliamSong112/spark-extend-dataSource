package com.cdp

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}


/**
  * 2017/04/05
  * cdp
  * 手动控制spark 消费 kafka的偏移度
  * 保证spark在意外退出时，重启程序数据不丢失
  */

object DealFlowBills2 {

  /** ***************************************************************************************************************
    * zookeeper 实例化，方便后面对zk的操作
    */
  val zk = ZkWork

  def main(args: Array[String]): Unit = {

    /** ***************************************************************************************************************
      * 输入参数
      */
    val Array(output, topic, broker, group, sec) = args

    /** ***************************************************************************************************************
      * spark套路
      */
    val conf = new SparkConf().setAppName("DealFlowBills2")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(sec.toInt))

    /** ***************************************************************************************************************
      * 准备kafka参数
      */
    val topics = Array(topic)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /** ***************************************************************************************************************
      * 判断zk中是否有保存过该计算的偏移量
      * 如果没有保存过,使用不带偏移量的计算,在计算完后保存
      * 精髓就在于KafkaUtils.createDirectStream这个地方
      * 默认是KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))，不加偏移度参数
      * 实在找不到办法，最后啃了下源码。发现可以使用偏移度参数
      */
    val stream = if (zk.znodeIsExists(s"${topic}offset")) {
      val nor = zk.znodeDataGet(s"${topic}offset")
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)//创建以topic，分区为k 偏移度为v的map

      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      println(s"[ DealFlowBills2 ] topic ${nor(0).toString}")
      println(s"[ DealFlowBills2 ] Partition ${nor(1).toInt}")
      println(s"[ DealFlowBills2 ] offset ${nor(2).toLong}")
      println(s"[ DealFlowBills2 ] zk中取出来的kafka偏移量★★★ $newOffset")
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
    } else {
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      println(s"[ DealFlowBills2 ] 第一次计算,没有zk偏移量文件")
      println(s"[ DealFlowBills2 ] 手动创建一个偏移量文件 ${topic}offset 默认从0偏移度开始计算")
      println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
      zk.znodeCreate(s"${topic}offset", s"$topic,$group,0")
      val nor = zk.znodeDataGet(s"${topic}offset")
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
    }

    /** ***************************************************************************************************************
      * 业务代码部分
      * 将流中的值取出来,用于计算
      */
    val lines = stream.map(_.value())
    lines.count().print()
    val result = lines
      .filter(_.split(",").length == 21)
      .map {
        mlines =>
          val line = mlines.split(",")
          (line(3), s"${line(4)},${line(2)}")
      }
      .groupByKey()
      .map {
        case (k, v) =>
          val result = v
            .flatMap {
              fmlines =>
                fmlines.split(",").toList.zipWithIndex
            }
            .groupBy(_._2)
            .map {
              case (v1, v2) =>
                v2.map(_._1)
            }
          (k, result)
      }

    /** ***************************************************************************************************************
      * 保存偏移度部分
      * （如果在计算的时候失败了，会接着上一次偏移度进行重算，不保存新的偏移度）
      * 计算成功后保存偏移度
      */
    stream.foreachRDD {
      rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition {
          iter =>
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
            println(s"[ DealFlowBills2 ]  topic: ${o.topic}")
            println(s"[ DealFlowBills2 ]  partition: ${o.partition} ")
            println(s"[ DealFlowBills2 ]  fromOffset 开始偏移量: ${o.fromOffset} ")
            println(s"[ DealFlowBills2 ]  untilOffset 结束偏移量: ${o.untilOffset} 需要保存的偏移量,供下次读取使用★★★")
            println(s"[ DealFlowBills2 ] --------------------------------------------------------------------")
            // 写zookeeper
            zk.offsetWork(s"${o.topic}offset", s"${o.topic},${o.partition},${o.untilOffset}")

          // 写本地文件系统
          // val fw = new FileWriter(new File("/home/hadoop1/testjar/test.log"), true)
          // fw.write(offsetsRangerStr)
          // fw.close()
        }
    }

    /** ***************************************************************************************************************
      * 最后结果保存到hdfs
      */
    result.saveAsTextFiles(output + s"/output/" + "010")

    /** ***************************************************************************************************************
      * spark streaming 开始工作
      */
    ssc.start()
    ssc.awaitTermination()

  }
}
