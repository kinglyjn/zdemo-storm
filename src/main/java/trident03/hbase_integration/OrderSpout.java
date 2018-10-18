package trident03.hbase_integration;

import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * OrderSpout
 * @author kinglyjn
 * @date 2018年8月17日
 *
 */
public class OrderSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private KafkaConsumer<String, String> consumer;
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.consumer = KafkaConsumerClient1.getKafkaConsumerOfSubscribed(Arrays.asList("test0817"));
	}

	@Override
	public void nextTuple() {
		ConsumerRecords<String, String> records = consumer.poll(200);
		for (ConsumerRecord<String, String> record : records) {
			String key = record.key();
			String message = record.value();
			collector.emit(new Values(message), key);
			System.err.println("[Thread-" + Thread.currentThread().getId() + ":OrderSpout-" + this.hashCode() + "] key=" + key + ", message=" + message);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
	
	@Override
	public void ack(Object msgId) {
		
	}

	@Override
	public void fail(Object msgId) {
		
	}
	
	@Override
	public void close() {
		KafkaConsumerClient1.close(consumer);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	@Override
	public void activate() {
		
	}

	@Override
	public void deactivate() {
		
	}
}
