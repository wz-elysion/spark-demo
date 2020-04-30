package wz_ling1991.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

@Slf4j
public class SparkStreamingDemo {

    private static String bootstrapServers = "MEGVII818534267:19192";
    private static String topic = "spark_streaming";
    private static String groupId = "spark_streaming_demo";

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming demo").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        jsc.sparkContext().setLogLevel("WARN");
        kafkaStream(jsc);
        jsc.start();
        new Thread(() -> {
            writeMsg(10000);
        }).start();
        jsc.awaitTermination();

    }

    public static void kafkaStream(JavaStreamingContext jsc) {
        Collection<String> topics = Arrays.asList(topic);
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, consumerConfig())
                );
        JavaDStream<Tuple2<String, String>> rs = stream.map(record -> new Tuple2<>(record.key(), record.value()));
        rs.foreachRDD(x -> {
            log.warn("x.size={}", x.count());
            x.collect().forEach(y -> log.warn("get message:{}", y));
        });
    }

    public static Map<String, Object> producerConfig() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "MEGVII818534267:19192");
        kafkaParams.put("key.serializer", StringSerializer.class);
        kafkaParams.put("value.serializer", StringSerializer.class);
        return kafkaParams;
    }

    public static Map<String, Object> consumerConfig() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        return kafkaParams;
    }


    public static void writeMsg(int size) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig());
        for (int i = 0; i < size; i++) {
            try {
                String v = UUID.randomUUID().toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, v);
                producer.send(record).get();
                log.warn("send success:v={}", v);
            } catch (Exception e) {
                log.error("send error:", e);
            }
        }
        producer.close();
    }


}