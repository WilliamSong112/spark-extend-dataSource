//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.scheduler.*;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.SparkContext;
////import org.apache.spark.status.AppStatusStore;
//import org.apache.spark.status.api.v1.ApplicationInfo;
//
//import java.util.Arrays;
//import java.util.List;
//
//import scala.Int;
//import scala.collection.Seq;
//
//import static java.util.Arrays.*;
//
//public class CustomSparkListener {
//
//    public static void main(String[] args) {
//
////        System.setProperty("HADOOP_USER_NAME","etluser");
//
////        SparkConf conf = new SparkConf();
////        conf.set("spark.hadoopRDD.ignoreEmptySplits", "true");
////        conf.set("spark.sql.adaptive.enabled", "true");
////        conf.set("spark.sql.adaptive.join.enabled", "true");
////        conf.set("spark.executor.memoryOverhead", "1024");
////        conf.set("spark.driver.memoryOverhead", "1024");
////        conf.set("spark.kryoserializer.buffer.max", "256m");
////        conf.set("spark.kryoserializer.buffer", "64m");
////        conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -Dlog4j.configuration=log4j.properties");
////        conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -Dlog4j.configuration=log4j.properties");
////        conf.set("spark.sql.parquet.writeLegacyFormat", "true");
//
//        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduce");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("testSparkListener")
//                .master("local")
//                .config(conf)
//                //.enableHiveSupport()
//                .getOrCreate();
//        spark.sparkContext().addSparkListener();
//        spark.sparkContext().parallelize(new Seq(52, 85));
////        SparkContext sc = new newSparkContext(conf);
//
//        //spark.sql("use coveroptimize");
//
////        spark.sparkContext().parallelize(Seq[Int](1,2,3,4,5))
//
//        List<Integer> number = Arrays.asList(3,2,5,4,7);
//        JavaRDD<Integer> numRDD = sc.parallelize(number);
//
//
//        numRDD.rdd();
//        System.out.println(numRDD.count());
//
//


//        JavaRDD<String> javaRDD;
//        javaRDD = spark.sparkContext().parallelize(asList(new String[] { "1", "2", "22", "2" }));
//
//        System.out.println(javaRDD.count());

//        val rdd1 = sc.parallelize(List(('a', 'c', 1), ('b', 'a', 1), ('b', 'd', 8)))
//        val rdd2 = sc.parallelize(List(('a', 'c', 2), ('b', 'c', 5), ('b', 'd', 6)))
//        val rdd3 = rdd1.union(rdd2).map {
//            x => {
//                Thread.sleep(500)
//                x
//            }
//        }.count()

//        AppStatusStore appStatusStore = spark.sparkContext().statusStore();
//        ApplicationInfo applicationInfo = appStatusStore.applicationInfo();
//        applicationInfo.memoryPerExecutorMB();

        //可以创建一个类实现Listener接口，然后调用该类实例。
        //这里测试，直接创建
//        sc.addSparkListener(new SparkListenerInterface() {
//            @Override
//            public void onExecutorRemoved( SparkListenerExecutorRemoved executorRemoved) {
//            }
//
//            /**
//             * Called when a stage completes successfully or fails, with information on the completed stage.
//             */
//            @Override
//            public void onStageCompleted( SparkListenerStageCompleted stageCompleted) {
//
//            }
//
//            @Override
//            public void onStageSubmitted( SparkListenerStageSubmitted stageSubmitted) {
//
//            }
//
//            @Override
//            public void onTaskStart(SparkListenerTaskStart taskStart) {
//
//            }
//            /**
//             * Called when a job ends
//             */
//            @Override
//            public void onJobEnd(SparkListenerJobEnd jobEnd) {
//                JobResult jobResult = jobEnd.jobResult();
//                System.err.println("自定义监听器jobEnd jobResult:"+jobResult);
//            }
//            /**
//             * Called when a job starts
//             */
//            @Override
//            public void onJobStart(SparkListenerJobStart jobStart) {
//                System.err.println("自定义监听器jobStart,jobId:"+jobStart.jobId());
//                System.err.println("自定义监听器jobStart,该job下stage数量："+jobStart.stageInfos().size());
//            }
//
//            @Override
//            public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
//
//            }
//
//            @Override
//            public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
//
//            }
//
//            @Override
//            public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
//
//            }
//            /**
//             * Called when the application ends
//             */
//            @Override
//            public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
//                System.err.println("Application结束，时间："+applicationEnd.time());
//            }
//
//            @Override
//            public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
//
//            }
//
//            @Override
//            public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
//
//            }
//
//            @Override
//            public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
//
//            }
//
//            @Override
//            public void onOtherEvent(SparkListenerEvent event) {
//
//            }
//
//            @Override
//            public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
//
//            }
//
////            @Override
////            public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
////
////            }
//
//            @Override
//            public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
//
//            }
//
//            @Override
//            public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
//
//            }
//            /**
//             * Called when the application starts
//             */
//            @Override
//            public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
//                System.err.println("Application启动，appName:"+applicationStart.appName()+",appID"+
//                        applicationStart.appId());
//            }
//
//            @Override
//            public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
//
//            }
//
//            @Override
//            public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
//
//            }
//
//            @Override
//            public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
//
//            }
//
//            @Override
//            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
//
//            }
//        });

//        String sql1 = "select roadid,count(1) cn from gridmappingroad group by roadid";
//        spark.sql(sql1).repartition(2).write().mode(SaveMode.Overwrite)
//                .saveAsTable("test_listener_table");
//
//        sc.stop();
//    }
//}