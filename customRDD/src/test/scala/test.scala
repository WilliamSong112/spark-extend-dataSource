//MyRDDTest.scala
//package org.apache.spark.myrdd {

  import org.apache.spark.{Partition, SparkContext, TaskContext}
  import scala.reflect.ClassTag
  import org.apache.spark.rdd._

  private[myrdd] class MapMyPartitionsRDD[U: ClassTag, T: ClassTag](
                                                                     var prev: RDD[T],
                                                                     f: (TaskContext, Int, Iterator[T]) => Iterator[U], // (TaskContext, partition index, iterator)
                                                                     preservesPartitioning: Boolean = false,
                                                                     isFromBarrier: Boolean = false,
                                                                     isOrderSensitive: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      println("my compute")
      f(context, split.index, firstParent[T].iterator(split, context))
    }

    override def clearDependencies(): Unit = {
      super.clearDependencies()
      prev = null
    }

//    @transient protected lazy override val isBarrier_ : Boolean =
//      isFromBarrier || dependencies.exists(_.rdd.isBarrier())

//    override protected def getOutputDeterministicLevel = {
//      if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
//        DeterministicLevel.INDETERMINATE
//      } else {
//        super.getOutputDeterministicLevel
//      }
//    }
  }

//  object DataSetImplicits {
//
//    implicit class MyRDDFunc[T: ClassTag](rdd: RDD[T]) extends Serializable {
//      def myMap[U: ClassTag](f: T => U): RDD[U] = {
//        println("my Map")
//        val cleanF = rdd.sparkContext.clean(f)
//        new MapMyPartitionsRDD[U, T](rdd, (_, _, iter) => iter.map(cleanF))
//      }
//    }
//  }



object MyRddTest {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession
      .builder
      .master("local[*]")
      .appName("MyRddTest")
      .getOrCreate()

    val rdd1 = spark.sparkContext.parallelize(1 to 10)

    import org.apache.spark.myrdd.DataSetImplicits._

    val output = rdd1.myMap(_ * 10)
    output.foreach(println)

    spark.stop()
  }

}

