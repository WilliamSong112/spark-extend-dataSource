//todo cdp 2017/04/06

package com.cdp

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

object ZkWork {
  val TIME_OUT = 5000
  var zooKeeper: ZooKeeper = _

  def watcher = new Watcher() {
    def process(event: WatchedEvent) {
      println(s"[ ZkWork ] process : " + event.getType)
    }
  }

  /** ***************************************************************************************************************
    * 基础方法
    * 连接zk,创建znode,更新znode
    */
  def connect() {
    println(s"[ ZkWork ] zk connect")
    zooKeeper = new ZooKeeper("hadoop1:2181,hadoop2:2181,hadoop3:2181,hadoop4:2181,hadoop5:2181", TIME_OUT, watcher)
  }

  def znodeCreate(znode: String, data: String) {
    println(s"[ ZkWork ] zk create /$znode , $data")
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  def znodeDataSet(znode: String, data: String) {
    println(s"[ ZkWork ] zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }

  /** ***************************************************************************************************************
    * 工作方法
    * 获得znode数据
    * 判断znode是否存在
    * 更新znode数据
    */
  def znodeDataGet(znode: String): Array[String] = {
    connect()
    println(s"[ ZkWork ] zk data get /$znode")
    try {
      new String(zooKeeper.getData(s"/$znode", true, null), "utf-8").split(",")
    } catch {
      case _: Exception => Array()
    }
  }

  def znodeIsExists(znode: String): Boolean ={
    connect()
    println(s"[ ZkWork ] zk znode is exists /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => false
      case _ => true
    }
  }

  def offsetWork(znode: String, data: String) {
    connect()
    println(s"[ ZkWork ] offset work /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => znodeCreate(znode, data)
      case _ => znodeDataSet(znode, data)
    }
    println(s"[ ZkWork ] zk close★★★")
    zooKeeper.close()
  }


}
