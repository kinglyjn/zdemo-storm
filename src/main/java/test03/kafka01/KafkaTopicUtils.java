package test03.kafka01;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import test00.config.ApplicationLoanConfig;

/**
 * topic增删改查工具类
 * 
 */
public class KafkaTopicUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicUtils.class);
	public static final String TOPIC_FORMAT_REGEX = "^[a-zA-Z_][a-zA-Z0-9_]*$";
	public static final String SEPARATOR_COMMAS = ",";
	
	/**
	 * 格式化topic
	 * 
	 */
	public static String formatTopic(String topic) {
		return topic.trim();
	}
	
	/**
	 * 验证topic是否合法
	 * 
	 */
	public static void checkTopic(String topic) {
		Topic.validate(topic);
		if (!Pattern.matches(TOPIC_FORMAT_REGEX, topic)) {
			throw new InvalidTopicException("[*] " + topic + " 名称非法！");
		}
	}
	
	/**
	 * 检查topic是否已经存在
	 * 
	 */
	public static Boolean isTopicExist(String topic) {
		return getAllTopic().contains(formatTopic(topic));
	}
	
	/**
	 * 查询所有topic
	 * 
	 */
	public static Set<String> getAllTopic() {
		Set<String> topicSet = new HashSet<String>();
		ZkUtils zkUtils = ZkUtils.apply(
				ApplicationLoanConfig.ZOOKEEPER_SERVERS+ApplicationLoanConfig.KAFKA_ZOOKEEPER_ROOT, 
				ApplicationLoanConfig.ZOOKEEPER_SESSION_TIMEOUT, 
				ApplicationLoanConfig.ZOOKEEPER_CONNECTION_TIMEOUT, 
				JaasUtils.isZkSecurityEnabled());
		String topics = zkUtils.getAllTopics().toString();
		
		for (String topic : topics.substring(7, topics.length() - 1).split(SEPARATOR_COMMAS)) {
			topicSet.add(formatTopic(topic));
		}
		return topicSet;
	}
	
	/**
	 * 创建topic
	 * 
	 */
	public static void createTopic(String topic) {
		if (!isTopicExist(topic)) {
			checkTopic(topic);
			ZkUtils zkUtils = ZkUtils.apply(
					ApplicationLoanConfig.ZOOKEEPER_SERVERS+ApplicationLoanConfig.KAFKA_ZOOKEEPER_ROOT, 
					ApplicationLoanConfig.ZOOKEEPER_SESSION_TIMEOUT, 
					ApplicationLoanConfig.ZOOKEEPER_CONNECTION_TIMEOUT, 
					JaasUtils.isZkSecurityEnabled());
			AdminUtils.createTopic(zkUtils, topic, 
					ApplicationLoanConfig.KAFKA_DEFAULT_PATITION, 
					ApplicationLoanConfig.KAFKA_DEFAULT_REPLICATION,
					new Properties(), RackAwareMode.Enforced$.MODULE$);
			zkUtils.close();
		} else {
			LOGGER.info("[*] topic {} 已存在", topic);
		}
	}
	
	/**
	 * 创建topic，并设定分区和副本数
	 * 
	 */
	public static void createTopic(String topic, int partition, int replication) {
		if (!isTopicExist(topic)) {
			checkTopic(topic);
			if (partition < 1) {
				throw new IllegalArgumentException("topic的partition(分区数)不能为空，且要是大于0的整数");
			}
			if (replication<1) {
				throw new IllegalArgumentException("topic的replication(副本数)不能为空，且要是大于0的整数");
			}
			ZkUtils zkUtils = ZkUtils.apply(
					ApplicationLoanConfig.ZOOKEEPER_SERVERS+ApplicationLoanConfig.KAFKA_ZOOKEEPER_ROOT, 
					ApplicationLoanConfig.ZOOKEEPER_SESSION_TIMEOUT, 
					ApplicationLoanConfig.ZOOKEEPER_CONNECTION_TIMEOUT, 
					JaasUtils.isZkSecurityEnabled());
			AdminUtils.createTopic(zkUtils, topic, partition, replication,
					new Properties(), RackAwareMode.Enforced$.MODULE$);
			zkUtils.close();
		} else {
			LOGGER.info("[*] topic {} 已存在", topic);
		}
	}
	
}
