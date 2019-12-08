package com.ywf.until;

import com.alibaba.fastjson.JSONObject;
import com.ywf.entry.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName KafkaUtils2
 * 往kafka中写数据
 * @Description KafkaUtils2
 * @Author YangWeiFeng
 * @Date 2019/12/8 17:40
 * @Version 1.0
 **/
public class KafkaUtils2 {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "student";  //kafka topic 需要和 flink 程序用同一个 topic

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "zhisheng" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSONObject.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSONObject.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
