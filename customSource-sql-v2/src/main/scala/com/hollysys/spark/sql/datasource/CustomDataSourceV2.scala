package com.hollysys.spark.sql.datasource

import java.util
import java.util.Optional

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
 * @author : shirukai
 * @date : 2019-01-30 10:37
 *       Spark SQL 基于DataSourceV2接口实现自定义数据源
 */

/**
 * 创建DataSource提供类
 * 1.继承DataSourceV2向Spark注册数据源
 * 2.继承ReadSupport支持读数据
 * 3.继承WriteSupport支持读数据
 */
class CustomDataSourceV2 extends DataSourceV2
  with ReadSupport
  with WriteSupport {

  /**
   * 创建Reader
   *
   * @param options 用户定义的options
   * @return 自定义的DataSourceReader
   */
  override def createReader(options: DataSourceOptions): DataSourceReader = new CustomDataSourceV2Reader(options)

  /**
   * 创建Writer
   *
   * @param jobId   jobId
   * @param schema  schema
   * @param mode    保存模式
   * @param options 用于定义的option
   * @return Optional[自定义的DataSourceWriter]
   */
  override def createWriter(jobId: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = Optional.of(new CustomDataSourceV2Writer)
}


/**
 * 自定义的DataSourceReader
 * 继承DataSourceReader
 * 重写readSchema方法用来生成schema
 * 重写createDataReaderFactories,用来根据条件，创建多个工厂实例
 *
 * @param options options
 */
class CustomDataSourceV2Reader(options: DataSourceOptions) extends DataSourceReader {
  /**
   * 生成schema
   *
   * @return schema
   */
  override def readSchema(): StructType = ???

  /**
   * 创建DataReader工厂实例
   *
   * @return 多个工厂类实例
   */
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._
    Seq(
      new CustomDataSourceV2ReaderFactory().asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }
}


/**
 * 自定义DataReaderFactory类
 */
class CustomDataSourceV2ReaderFactory extends DataReaderFactory[Row] {
  /**
   * 重写createDataReader方法，用来实例化自定义的DataReader
   *
   * @return 自定义的DataReader
   */
  override def createDataReader(): DataReader[Row] = new CustomDataReader
}


/**
 * 自定义DataReader类
 */
class CustomDataReader extends DataReader[Row] {
  /**
   * 是否有下一条数据
   *
   * @return boolean
   */
  override def next(): Boolean = ???

  /**
   * 获取数据
   * 当next为true时会调用get方法获取数据
   *
   * @return Row
   */
  override def get(): Row = ???

  /**
   * 关闭资源
   */
  override def close(): Unit = ???
}

/**
 * 自定义DataSourceWriter
 * 继承DataSourceWriter
 */
class CustomDataSourceV2Writer extends DataSourceWriter {
  /**
   * 创建WriterFactory
   *
   * @return 自定义的DataWriterFactory
   */
  override def createWriterFactory(): DataWriterFactory[Row] = ???

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
class CustomDataWriterFactory extends DataWriterFactory[Row] {
  /**
   * 创建DataWriter
   *
   * @param partitionId   分区ID
   * @param attemptNumber 重试次数
   * @return DataWriter
   *         每个分区创建一个RestDataWriter实例
   */
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = ???
}
/**
 * DataWriter
 */
class CustomDataWriter extends DataWriter[Row] {
  /**
   * write
   *
   * @param record 单条记录
   *               每条记录都会触发该方法
   */
  override def write(record: Row): Unit = ???
  /**
   * commit
   *
   * @return commit message
   *         每个分区触发一次
   */
  override def commit(): WriterCommitMessage = ???


  /**
   * 回滚：当write发生异常时触发该方法
   */
  override def abort(): Unit = ???
}

