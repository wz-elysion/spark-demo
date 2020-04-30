package wz_ling1991.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import lombok.Data;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;


public class SparkMongo {

    public static void main(final String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://adminx:alFTwfsnvKF@10.231.50.14:27027/archives.learning_spark")
                .config("spark.mongodb.output.uri", "mongodb://adminx:alFTwfsnvKF@10.231.50.14:27027/archives.learning_spark")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        /*Start Example: Read data from MongoDB************************/
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(rdd.count());

        Dataset<Row> implicitDS = rdd.toDF();
        implicitDS.printSchema();
        implicitDS.show();

        // Load data with explicit schema
        Dataset<MongoDemo> explicitDS = rdd.toDS(MongoDemo.class);
        explicitDS.printSchema();
        explicitDS.show();

        // Create the temp view and execute the query
        explicitDS.createOrReplaceTempView("mongoDemo");
        Dataset<Row> centenarians = spark.sql("SELECT * FROM mongoDemo where test > 5");
        centenarians.show();


//        JavaRDD<Document> documents = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map(
//                x -> Document.parse("{test:" + x + "}"));
//        MongoSpark.save(documents);
//        JavaPairRDD<Document, Document> pairRdd = documents.mapToPair(x -> new Tuple2<>(x, x));
        jsc.close();

    }
}

@Data
class MongoDemo {
    private String _id;
    private String test;
}
