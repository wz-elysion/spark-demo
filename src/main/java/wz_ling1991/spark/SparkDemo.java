package wz_ling1991.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDemo {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Learning demo").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("WARN");
        JavaRDD<String> inputA = javaSparkContext.textFile("./src/main/resources/stage1-groupByKey");
        JavaRDD<String> inputC = javaSparkContext.textFile("./src/main/resources/stage2-map");
        JavaRDD<String> inputE = javaSparkContext.textFile("./src/main/resources/stage2-union");
        JavaPairRDD<String, Iterable<String>> stage1_B = inputA.mapToPair(x -> new Tuple2<>(x.split(":")[0], x.split(":")[1]))
                .groupByKey();
        JavaPairRDD<String, String> stage2_D = inputC.mapToPair(x -> new Tuple2<>(x.split(":")[0], x.split(":")[1]))
                .mapToPair(x -> new Tuple2<>(x._1, "stage-2-" + x._2));
        JavaPairRDD<String, String> stage2_E = inputE.mapToPair(x -> new Tuple2<>(x.split(":")[0], x.split(":")[1]));
        JavaPairRDD<String, String> stage2_F = stage2_D.union(stage2_E);
        JavaPairRDD<String, Tuple2<Iterable<String>, String>> stage3 = stage1_B.join(stage2_F);
        stage3.count();
        while (true) {

        }
    }

}