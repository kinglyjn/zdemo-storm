package trident03.hbase_integration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 消息消费者，自动周期性offset
 * @author zhangqingli
 *
 */
public class KafkaConsumerClient1 {
	
	/**
	 * 获取配置参数
	 * 
	 */
	private static Properties getConfiguration() {
		Properties props = new Properties();
		
		// bootstrap.servers 需要本地设置hosts路由映射
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bd117:9092,bd118:9092,bd119:9092");
		// group.id
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group01");
		// enable.auto.commit & auto.commit.interval.ms 设置自动offset，offset的值在0.10.1.1版本以后默认存放在__consumer_offsets主题中
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
		// request.timeout.ms
		props.put("request.timeout.ms", "305000"); 
		// session.timeout.ms
		props.put("session.timeout.ms", "10000");
		// max.poll.interval.ms
		props.put("max.poll.interval.ms", "300000"); 
		// max.poll.records 一次从kafka中poll出来的数据条数，这些数据需要在在session.timeout.ms这个时间内处理完
		props.put("max.poll.records", 500);
		// auto.offset.reset，枚举值为earliest|latest|none
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		// key.deserializer
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		// value.deserializer
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		return props;
	}
	
	/**
	 * 获取 KafkaConsumer
	 * 
	 */
	public static <K,V> KafkaConsumer<K,V> getKafkaConsumerOfSubscribed(List<String> topics) {
		KafkaConsumer<K,V> consumer = new KafkaConsumer<>(getConfiguration());
		consumer.subscribe(topics);
		return consumer;
	}
	public static <K,V> KafkaConsumer<K,V> getKafkaConsumerOfAssigned(Collection<TopicPartition> partitions) {
		KafkaConsumer<K,V> consumer = new KafkaConsumer<>(getConfiguration());
		consumer.assign(partitions);
		return consumer;
	}
	
	/**
	 * 关闭 KafkaConsumer
	 * 
	 */
	public static <K,V> void close(KafkaConsumer<K,V> consumer) {
		consumer.close();
	}
	
	
	/*
	 * main
	 * 
	 */
	public static void main(String[] args) {
		List<String> topics = Arrays.asList("test0817");
		KafkaConsumer<String, String> consumer = getKafkaConsumerOfSubscribed(topics);
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500); //获取缓冲区消息的超时时间，如果设置为0则立即获取缓冲区的所有消息
			for (ConsumerRecord<String, String> record : records) {
				System.err.printf("[Consumer] partition=%d, offset=%d, key=%s, value=%s%n", record.partition(), record.offset(), record.key(), record.value());
			}
		}
	}
}
