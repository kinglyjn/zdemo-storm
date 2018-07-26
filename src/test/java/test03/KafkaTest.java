package test03;

import java.util.Date;

import org.junit.Test;

import scala.util.Random;
import test03.kafka01.ApplicationLoanRecord;
import test03.kafka01.KafkaMessage;
import test03.kafka01.KafkaSimpleProducerClient;

/**
 * kafka测试类
 *
 */
public class KafkaTest {
	
	/*
		kafka.bootstrap.servers= 192.168.1.87:6300,192.168.1.88:6300,192.168.1.89:6300
		kafka.zookeeper.connect= 192.168.1.84:6200,192.168.1.85:6200,192.168.1.86:6200
		kafka.zookeeper.rootDir= /kafka
	 */
	@Test
	public void testCreateTopic() {
		//KafkaTopicUtils.createTopic("im_application", 3, 1);
		//KafkaTopicUtils.createTopic("im_loan", 3, 1);
	}
	
	@Test
	public void testProduceMsg() {
		String[] topics = new String[] {"im_application", "im_loan"};
		String subType = "application_loan";
		String[] types = new String[] {"apply", "withdraw"};
		String[] pipelineNames = new String[] {"Android A", "Android B", "IOS A", "IOS B"};
		String[] levelIds = new String[] {"A", "B", "B+"};
		
		for (long i = 0; i < 100; i++) {
			ApplicationLoanRecord record = new ApplicationLoanRecord();
			record.setType(types[new Random().nextInt(types.length)]);
			record.setPipelineName(pipelineNames[new Random().nextInt(pipelineNames.length)]);
			record.setLevelId(levelIds[new Random().nextInt(levelIds.length)]);
			record.setCreateTime(new Date().getTime());
			record.setEffectiveTime(new Date().getTime());
			
			if ("apply".equals(record.getType())) {
				KafkaMessage<ApplicationLoanRecord> km = new KafkaMessage<>(topics[0], subType, record);
				KafkaSimpleProducerClient.produceMsg(km, null);
			} else if ("withdraw".equals(record.getType())) {
				KafkaMessage<ApplicationLoanRecord> km = new KafkaMessage<>(topics[1], subType, record);
				KafkaSimpleProducerClient.produceMsg(km, null);
			}
		}
	}
	
}
