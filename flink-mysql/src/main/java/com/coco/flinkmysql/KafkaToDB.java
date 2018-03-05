package com.coco.flinkmysql;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;

public class KafkaToDB {

    @Value("kafka.hosts")
    static String servers = "localhost:9092";
    @Value("kafka.zookper")
    static String connect = "localhost:2181";
    @Value("kafka.group")
    static String goup = "group";
    @Value("kafka.topic")
    static String topic = "mytopic";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("zookeeper.connect", connect);
        props.put("group.id", goup);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000);

        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer08<String>(topic, new SimpleStringSchema(), props));

        DataStream<Tuple3<Integer, String, Integer>> sourceStreamTra = sourceStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return StringUtils.isNotBlank(value);
            }
        }).map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple3<Integer, String, Integer> map(String value)
                    throws Exception {
                System.err.println("map -> " + value);
                String[] args = value.split(":");
                return new Tuple3<Integer, String, Integer>(Integer.valueOf(args[0]), args[1], Integer.valueOf(args[2]));
            }
        });

        sourceStreamTra.addSink(new MySQLSink());
        env.execute("data to mysql start");
    }

}
