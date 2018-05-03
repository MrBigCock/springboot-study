import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) {
        /**
         * 这个例子中，每次调用都会创建一个Producer实例，但此处只是为了演示方便，实际使用中，请将Producer作为单例使用，它是线程安全的。

         * 从Kafka 0.11 开始，KafkaProducer支持两种额外的模式：幂等(idempotent)与事务(transactional)。幂等使得之前的at least once变成exactly once传送
         * 幂等Producer的重试不再会导致重复消息。事务允许应用程序以原子方式将消息发送到多个分区（和主题！）

         * 开启idempotence幂等:props.put("enable.idempotence", true);设置之后retries属性自动被设为Integer.MAX_VALUE;;acks属性自动设为all;;max.inflight.requests.per.connection属性自动设为1.其余一样。

         * 开启事务性： props.put("transactional.id", "my-transactional-id");一旦这个属性被设置，那么幂等也会自动开启。然后使用事务API操作即可
         */

        sendInTx();
    }

    private static void send() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.idempotence", true);//开启idempotence幂等 extract-once
//         props.put("acks", "all");//acks配置控制请求被认为完成的条件
//         props.put("retries", 0);重试次数
//         props.put("batch.size", 16384);
//         props.put("linger.ms", 1);
//         props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

        producer.close();
    }

    private static void sendInTx() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", "my-transactional-id");//要启用事务，必须配置一个唯一的事务id
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         * http://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
         * KafkaProducer类是线程安全的，可以在多线程之间共享。
         */
        Producer producer = new KafkaProducer(props);

        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++) {
                // send()是异步的，会立即返回，内部是缓存到producer的buffer中，以便于生产者可以批量提交， 你也可以传递一个回调send(ProducerRecord<K,V> record, Callback callback)
                producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            //无法恢复的异常，我们只能关闭producer
            producer.close();
        } catch (KafkaException e) {
            // 可恢复的异常，终止事务然后重试即可。
            producer.abortTransaction();
        }
        producer.close();
    }
}
