package wz_ling1991.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class PageRankDemo {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("PageRank demo").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Link> linkList = Arrays.asList(
                new Link("A", Arrays.asList("D")),
                new Link("B", Arrays.asList("A")),
                new Link("C", Arrays.asList("A", "B")),
                new Link("D", Arrays.asList("A", "C")));
        JavaPairRDD<String, List<String>> links = jsc.parallelize(linkList).mapToPair(x -> new Tuple2<>(x.url, x.links));
        JavaPairRDD<String, Double> ranks = jsc.parallelize(Arrays.asList("A", "B", "C", "D")).mapToPair(x -> new Tuple2<>(x, 1.0));
        for (int i = 0; i < 20; i++) {
            ranks = links.join(ranks).flatMap(x -> {
                List<String> link = x._2._1;
                return link.stream().map(dest -> new Tuple2<>(dest, x._2._2 / x._2._1.size())).iterator();
            }).mapToPair(x -> new Tuple2<>(x._1, x._2)).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
            ranks.sortByKey().coalesce(1).foreach(x -> FileUtils.writeStringToFile(new File("./pageRank"), x._1 + ":" + String.format("%.2f", x._2) + "    ", "utf-8", true));
            FileUtils.writeStringToFile(new File("./pageRank"), "\n", "utf-8", true);
//            这里可以直接保存到hdfs上
            ranks.saveAsTextFile("./pageRankHdfs" + i);
        }
        while (true) {

        }
    }

    static class Link implements Serializable {
        String url;
        List<String> links;

        public Link(String url, List<String> links) {
            this.url = url;
            this.links = links;
        }
    }
}