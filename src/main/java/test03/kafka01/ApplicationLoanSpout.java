package test03.kafka01;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import test00.config.ApplicationLoanConfig;
import utils.NCUtil;

/**
 * spout
 * 
 * 	发送的 countField 举例：
 * 		apply:20180109:pipeline:AndroidA:12
 * 		...
 *  		loan:20180109:pipeline:AndroidA:12
 *  		loan:20180109:levelid:A:12
 * 
 */
public class ApplicationLoanSpout implements IRichSpout {
	private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationLoanSpout.class);
	private static final long serialVersionUID = 1L;
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMdd:HH");

	private KafkaConsumer<String, String> kafkaConsumer;
	private SpoutOutputCollector collector;
	private Map<String, String> toSendMsgs = new ConcurrentHashMap<>();
	private Map<String, String> allMsgs = new ConcurrentHashMap<>();
	private Map<String, Integer> failCountMap = new ConcurrentHashMap<>();
	
	
	public KafkaConsumer<String, String> newConsumer() {
 		Properties props = new Properties();
		props.put("bootstrap.servers", ApplicationLoanConfig.KAFKA_BOOTSTRAP_SERVERS);
		props.put("group.id", ApplicationLoanConfig.KAFKA_CUSTOMER_GROUP); 
		props.put("enable.auto.commit", ApplicationLoanConfig.KAFKA_CUSTOMER_ENABLE_AUTO_COMMIT);
		props.put("max.poll.records", 20); //默认拉取记录是500，直到0.10版本才引入该参数，所以在0.9版本配置是无效的
		//props.put("max.poll.interval.ms", 300000); 	//提取消息的最大间隔时间，超过此间隔则重新rebalance
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(ApplicationLoanConfig.KAFKA_TOPICS)); //同属于一个消费组
		return consumer;
 	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.kafkaConsumer = newConsumer();
		this.collector = collector;
	}

	@Override
	public void close() {
		LOGGER.info("[*] call close");
		kafkaConsumer.close();
	}

	@Override
	public void activate() {
		LOGGER.info("[*] call activate");
	}

	@Override
	public void deactivate() {
		LOGGER.info("[*] call deactivate");
	}

	private Map<String, String> initCountFields(KafkaMessage<ApplicationLoanRecord> kmsg) {
		Map<String, String> countFields = new HashMap<>(2);
		ApplicationLoanRecord record = kmsg.getT();
		
		String type = record.getType();
		if ("apply".equals(type)) {
			Long createTime = record.getCreateTime();
			String[] dayAndHours = SDF.format(new Date(createTime)).split(":");
			String day = dayAndHours[0];
			String hour = dayAndHours[1];
			String pipelineName = record.getPipelineName();
			
			//apply:20180109:pipeline:AndroidA:12
			String pipelineCountType = ApplicationLoanConfig.TYPE_APPLY
						+ ApplicationLoanConfig.DEFAULT_SEPARATOR + day
						+ ApplicationLoanConfig.DEFAULT_SEPARATOR + ApplicationLoanConfig.COLUMN_PIPELINE
						+ ApplicationLoanConfig.DEFAULT_SEPARATOR + pipelineName
						+ ApplicationLoanConfig.DEFAULT_SEPARATOR + hour;
			countFields.put(ApplicationLoanConfig.COUNT_TYPE_APPLY_PIPELINE, pipelineCountType);
		} else if ("withdraw".equals(type)) {
			Long effectiveTime = record.getEffectiveTime();
			String[] dayAndHours = SDF.format(new Date(effectiveTime)).split(":");
			String day = dayAndHours[0];
			String hour = dayAndHours[1];
			String pipelineName = record.getPipelineName();
			String levelId = record.getLevelId();
			
			//loan:20180109:pipeline:AndroidA:12
			String pipelineCountType = ApplicationLoanConfig.TYPE_LOAN
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + day
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + ApplicationLoanConfig.COLUMN_PIPELINE
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + pipelineName
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + hour;
			//loan:20180109:levelid:A:12
			String levelidCountType = ApplicationLoanConfig.TYPE_LOAN
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + day
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + ApplicationLoanConfig.COLUMN_LEVELID
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + levelId
					+ ApplicationLoanConfig.DEFAULT_SEPARATOR + hour;
			countFields.put(ApplicationLoanConfig.COUNT_TYPE_LOAN_PIPELINE, pipelineCountType);
			countFields.put(ApplicationLoanConfig.COUNT_TYPE_LOAN_LEVELID, levelidCountType);
		}
		return countFields;
	}

	@Override
	public void nextTuple() {
		if (!toSendMsgs.isEmpty()) {
			// 重新处理失败消息
			for (Entry<String, String> entry : toSendMsgs.entrySet()) {
				collector.emit(new Values(entry.getValue()), entry.getKey());
			}
			toSendMsgs.clear();
		} else {
			// 消费kafka消息
			ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
			for (TopicPartition topicPartition : records.partitions()) {
				List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
				for (int i=0; i<partitionRecords.size(); i++) {
					String kafkaMessageJson = partitionRecords.get(i).value();
					KafkaMessage<ApplicationLoanRecord> kmsg = JSON.parseObject(kafkaMessageJson, new TypeReference<KafkaMessage<ApplicationLoanRecord>>(){});
					kmsg.setPartition(topicPartition.partition());
					
					Map<String, String> countFields = initCountFields(kmsg);
					for (Entry<String,String> countFieldEntry : countFields.entrySet()) {
						// msgid:
						// im_application:1:23:1534832911:apply_pipeline
						// im_loan:1:23:1534832911:loan_pipeline
						// im_loan:1:23:1534832911:loan_levelid
						String msgId= kmsg.getTopic()
								+ ApplicationLoanConfig.DEFAULT_SEPARATOR + kmsg.getPartition() 
								+ ApplicationLoanConfig.DEFAULT_SEPARATOR + partitionRecords.get(i).offset() 
								+ ApplicationLoanConfig.DEFAULT_SEPARATOR + kmsg.getTimestamp()
								+ ApplicationLoanConfig.DEFAULT_SEPARATOR + countFieldEntry.getKey();
						// countField:
						// apply:20180109:pipeline:AndroidA:12
						// loan:20180109:pipeline:AndroidA:12
						// loan:20180109:levelid:A:12
						allMsgs.put(msgId, countFieldEntry.getValue());
						NCUtil.write2NC(this, "call spout nextTuple, and msgId=" + msgId);
						collector.emit(new Values(countFieldEntry.getValue()), msgId);
					}
					kafkaConsumer.commitSync();
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGGER.info("[*] call declareOutputFields");
		declarer.declare(new Fields("countField"));
	}
	
	@Override
	public void ack(Object msgId) {
		NCUtil.write2NC(this, "call spout ack, and msgId=" + msgId);
		
		String mid = (String) msgId;
		toSendMsgs.remove(mid);
		failCountMap.remove(mid);
		allMsgs.remove(mid);
	}

	/**
	 * Spout消息处理一条消息失败后
	 * 1. spout 进行 SPOUT_MAX_RETRY次 的重试
	 * 2. soupt重试失败之后，重置 kafka offset，重新从kafka消费这条消息
	 * 
	 */
	@Override
	public void fail(Object msgId) {
		NCUtil.write2NC(this, "call spout fail, and msgId=" + msgId);
		
		String mid = (String) msgId;
		Integer count = failCountMap.get(mid);
		count = (count==null) ? 0 : count;
		if (count < ApplicationLoanConfig.SPOUT_MAX_RETRIES) { //消息失败的次数
			failCountMap.put(mid, ++count);
			toSendMsgs.put(mid, allMsgs.get(mid));
		} else {
			LOGGER.info("已经超过最大重试次数，msgid=" + mid);
			NCUtil.write2NC(this, "已经超过最大重试次数，msgid=" + mid);
			
			toSendMsgs.remove(mid);
			failCountMap.remove(mid);
			allMsgs.remove(mid);
			
			// im_application:1:23:1534832911:apply_pipeline
			/*
			String[] splits = mid.split(ApplicationLoanConfig.DEFAULT_SEPARATOR);
			String topic = splits[0];
			Integer partition = Integer.parseInt(splits[1]);
			Long needToOffset = Long.parseLong(splits[2]);
			kafkaConsumer.seek(new TopicPartition(topic, partition), needToOffset);
			*/
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
