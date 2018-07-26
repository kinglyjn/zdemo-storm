package test03.kafka01;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSON;

import test00.config.ApplicationLoanConfig;

/**
 * 生产者客户端
 *
 */
public class KafkaSimpleProducerClient {
	private static Producer<String, String> producer;
	static {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationLoanConfig.KAFKA_BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.ACKS_CONFIG, "1");   		//all需要leader和所有replica回执确认 0不需要回执确认 1仅需要leader回执确认
		props.put(ProducerConfig.RETRIES_CONFIG, 1);  		//重试一次
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); 	//空间上buffer满足>16k即发送消息
		props.put(ProducerConfig.LINGER_MS_CONFIG, 5);      	//时间上满足>5ms即发送消息，时间限制和空间限制只要满足其一即可发送消息
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1073741824); //1G
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		//props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "test01.producer.MyPartitioner"); //指定分区
		producer = new KafkaProducer<String,String>(props);
	}
	
	/**
	 * 发送自定义包装的消息
	 */
	public static <T> void produceMsg(KafkaMessage<T> km, Callback callback) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(km.getTopic(), km.getTimestamp(), JSON.toJSONString(km));
		if (callback==null) {
			producer.send(record);
		} else {
			producer.send(record, callback);
		}
		producer.flush();
	}
	
	
	/*public static void main(String[] args) {
		Map<String, String> user = Collections.singletonMap("name", "zhangsan");
		KafkaMessage<Map<String,String>> km = new KafkaMessage<>("test", "sub1", user);
		KafkaSimpleProducerClient.produceMsg(km, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				System.out.println("[*] call ack");
			}
		});
	}*/
}
