package trident03.hbase_integration;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSON;

/**
 * 生产者客户端
 *
 */
public class KafkaProducerClient {
	private static Producer<String, String> producer = new KafkaProducer<String,String>(getConfiguration());
	
	
	/**
	 * 获取配置参数
	 * 
	 */
	public static Properties getConfiguration() {
		Properties props = new Properties();
		
		// bootstrap.servers
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bd117:9092,bd118:9092,bd119:9092");
		// 0不需要回执确认; 1仅需要leader回执确认; all[或者-1]需要leader和所有replica回执确认
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		// 重试一次
		props.put(ProducerConfig.RETRIES_CONFIG, 1); 
		// 空间上buffer满足>16k即发送消息
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		// 时间上满足>5ms即发送消息，时间限制和空间限制只要满足其一即可发送消息
		props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
		// buffer.memory 1G
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1073741824);
		// key.serializer
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// value.serializer
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// partitioner.class
		//props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "test01.producer.MyPartitioner"); //指定分区
		
		return props;
	}
	
	
	/**
	 * 发送自定义包装的消息
	 * 
	 */
	public static <M> void produceMsg(KafkaMessage<M> message, Callback callback) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(message.getTopic(), message.getKey(), JSON.toJSONString(message));
		if (callback==null) {
			producer.send(record);
		} else {
			producer.send(record, callback);
		}
		producer.flush();
	}
	
	
	/*
	 * main
	 * 
	 */
	public static void main(String[] args) throws InterruptedException {
		String topicName = "test0817";
		String subType = "test";
		
		int i = 0;
		Random random = new Random();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[] orderAmt = {"10.10", "20.10", "50.2", "60.0", "80.0"};
		String[] areaId = {"1", "2", "3", "4", "5"};
		while (true) {
			KafkaMessage<String> message = new KafkaMessage<String>(topicName, subType, 
					Integer.toString(i++),
					orderAmt[random.nextInt(orderAmt.length)]+"\t"+sdf.format(new Date())+"\t"+areaId[random.nextInt(areaId.length)]);
			System.out.println(message);
			//
			KafkaProducerClient.produceMsg(message, null);
			Thread.sleep(1000);
		}
	}
}
