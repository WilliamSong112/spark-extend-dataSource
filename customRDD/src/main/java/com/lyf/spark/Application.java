package com.lyf.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.IntegerRDD;

/**
 * 应用启动
 *
 * @author yufei.liu
 */
public class Application {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-custom-RDD");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        JavaRDD<Integer> javaRDD1 = new IntegerRDD(sparkContext.sc()).toJavaRDD();
        JavaRDD<Integer> javaRDD2 = new IntegerRDD(sparkContext.sc()).toJavaRDD();
        long count = javaRDD1.union(javaRDD2).distinct().count();
        System.out.println(count);

        sparkContext.close();
    }

}