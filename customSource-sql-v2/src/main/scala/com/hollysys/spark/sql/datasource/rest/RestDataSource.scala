package com.hollysys.spark.sql.datasource.rest

import java.math.BigDecimal
import java.util
import java.util.Optional

import com.alibaba.fastjson.{JSONArray, JSONObject, JSONPath}
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
 * @author : shirukai
 * @date : 2019-01-09 16:53
 *       基于Rest的Spark SQL DataSource
 */
class RestDataSource extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader =
    new RestDataSourceReader(
      options.get("url").get(),
      options.get("params").get(),
      options.get("xPath").get(),
      options.get("schema").get()
    )

  override def createWriter(jobId: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = Optional.of(new RestDataSourceWriter)
}

/**
 * 创建RestDataSourceReader
 *
 * @param url          REST服务的的api
 * @param params       请求需要的参数
 * @param xPath        JSON数据的xPath
 * @param schemaString 用户传入的schema字符串
 */
class RestDataSourceReader(url: String, params: String, xPath: String, schemaString: String)
  extends DataSourceReader {
  // 使用StructType.fromDDL方法将schema字符串转成StructType类型
  var requiredSchema: StructType = StructType.fromDDL(schemaString)

  /**
   * 生成schema
   *
   * @return schema
   */
  override def readSchema(): StructType = requiredSchema

  /**
   * 创建工厂类
   *
   * @return List[实例]
   */
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._
    Seq(
      new RestDataReaderFactory(url, params, xPath).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }


}

/**
 * RestDataReaderFactory工厂类
 *
 * @param url    REST服务的的api
 * @param params 请求需要的参数
 * @param xPath  JSON数据的xPath
 */
class RestDataReaderFactory(url: String, params: String, xPath: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new RestDataReader(url, params, xPath)
}

/**
 * RestDataReader类
 *
 * @param url    REST服务的的api
 * @param params 请求需要的参数
 * @param xPath  JSON数据的xPath
 */
class RestDataReader(url: String, params: String, xPath: String) extends DataReader[Row] {
  // 使用Iterator模拟数据
  val data: Iterator[Seq[AnyRef]] = getIterator

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): Row = {
    val seq = data.next().map {
      // 浮点类型会自动转为BigDecimal，导致Spark无法转换
      case decimal: BigDecimal =>
        decimal.doubleValue()
      case x => x
    }
    Row(seq: _*)
  }

  override def close(): Unit = {
    println("close source")
  }

  def getIterator: Iterator[Seq[AnyRef]] = {
    import scala.collection.JavaConverters._
    val res: List[AnyRef] = RestDataSource.requestData(url, params, xPath)
    res.map(r => {
      r.asInstanceOf[JSONObject].asScala.values.toList
    }).toIterator
  }
}

/** *
 * RestDataSourceWriter
 */
class RestDataSourceWriter extends DataSourceWriter {
  /**
   * 创建RestDataWriter工厂类
   *
   * @return RestDataWriterFactory
   */
  override def createWriterFactory(): DataWriterFactory[Row] = new RestDataWriterFactory

  /**
   * commit
   *
   * @param messages 所有分区提交的commit信息
   *                 触发一次
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  /** *
   * abort
   *
   * @param messages 当write异常时调用
   */
  override def abort(messages: Array[WriterCommitMessage]): Unit = ???

}

/**
 * DataWriterFactory工厂类
 */
class RestDataWriterFactory extends DataWriterFactory[Row] {
  /**
   * 创建DataWriter
   *
   * @param partitionId   分区ID
   * @param attemptNumber 重试次数
   * @return DataWriter
   *         每个分区创建一个RestDataWriter实例
   */
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = new RestDataWriter(partitionId, attemptNumber)
}

/**
 * RestDataWriter
 *
 * @param partitionId   分区ID
 * @param attemptNumber 重试次数
 */
class RestDataWriter(partitionId: Int, attemptNumber: Int) extends DataWriter[Row] {
  /**
   * write
   *
   * @param record 单条记录
   *               每条记录都会触发该方法
   */
  override def write(record: Row): Unit = {

    println(record)
  }

  /**
   * commit
   *
   * @return commit message
   *         每个分区触发一次
   */
  override def commit(): WriterCommitMessage = {
    RestWriterCommitMessage(partitionId, attemptNumber)
  }

  /**
   * 回滚：当write发生异常时触发该方法
   */
  override def abort(): Unit = {
    println("abort 方法被出发了")
  }
}

case class RestWriterCommitMessage(partitionId: Int, attemptNumber: Int) extends WriterCommitMessage

object RestDataSource {
  def requestData(url: String, params: String, xPath: String): List[AnyRef] = {
    import scala.collection.JavaConverters._
    val response = Request.Post(url).bodyString(params, ContentType.APPLICATION_JSON).execute()
    JSONPath.read(response.returnContent().asString(), xPath)
      .asInstanceOf[JSONArray].asScala.toList
  }
}

object RestDataSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val df = spark.read
      .format("com.hollysys.spark.sql.datasource.rest.RestDataSource")
      .option("url", "http://model-opcua-hollysysdigital-test.hiacloud.net.cn/aggquery/query/queryPointHistoryData")
      .option("params", "{\n    \"startTime\": \"1543887720000\",\n    \"endTime\": \"1543891320000\",\n    \"maxSizePerNode\": 1000,\n    \"nodes\": [\n        {\n            \"uri\": \"/SymLink-10000012030100000-device/5c174da007a54e0001035ddd\"\n        }\n    ]\n}")
      .option("xPath", "$.result.historyData")
      //`response` ARRAY<STRUCT<`historyData`:ARRAY<STRUCT<`s`:INT,`t`:LONG,`v`:FLOAT>>>>
      .option("schema", "`s` INT,`t` LONG,`v` DOUBLE")
      .load()


    df.printSchema()
    df.show(false)
    //    df.repartition(5).write.format("com.hollysys.spark.sql.datasource.rest.RestDataSource")
    //      .save()
  }
}
