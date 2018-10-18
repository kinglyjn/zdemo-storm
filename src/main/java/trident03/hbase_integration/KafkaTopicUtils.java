package trident03.hbase_integration;

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

/**
 * topic增删改查工具类
 * 
 */
public class KafkaTopicUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicUtils.class);
	public static final String TOPIC_FORMAT_REGEX = "^[a-zA-Z][a-zA-Z0-9]*$";
	public static final String SEPARATOR_COMMAS = ",";
	private static final String ZK_CONNECTION = "bd117:2181,bd118:2181,bd119:2181/kafka";
	private static final Integer SESSION_TIMEOUT = 30000;
	private static final Integer CONNECTION_TIMEOUT = 30000;
	private static final Integer DEFAULT_PARTITION_NUM = 3;
	private static final Integer DEFAULT_REPLICATION_NUM = 1;
	
	
	/**
	 * 格式化topic
	 */
	public static String formatTopic(String topic) {
		return topic.trim();
	}
	
	/**
	 * 验证topic是否合法
	 */
	public static void checkTopic(String topic) {
		Topic.validate(topic);
		if (!Pattern.matches(TOPIC_FORMAT_REGEX, topic)) {
			throw new InvalidTopicException("[*] " + topic + " 名称非法，只能含有大小字母与数字，且只能大小字母开头.");
		}
	}
	
	/**
	 * 检查topic是否已经存在
	 */
	public static Boolean isTopicExist(String topic) {
		return getAllTopics().contains(formatTopic(topic));
	}
	
	/**
	 * 查询所有topic
	 */
	public static Set<String> getAllTopics() {
		Set<String> topicSet = new HashSet<String>();
		ZkUtils zkUtils = ZkUtils.apply(ZK_CONNECTION, SESSION_TIMEOUT, CONNECTION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
		String topics = zkUtils.getAllTopics().toString();
		if (topics.length() <= 8) {
			return topicSet;
		}
		
		for (String topic : topics.substring(7, topics.length() - 1).split(SEPARATOR_COMMAS)) {
			topicSet.add(formatTopic(topic));
		}
		return topicSet;
	}
	
	/**
	 * 创建topic
	 */
	public static void createTopic(String topic) {
		if (!isTopicExist(topic)) {
			checkTopic(topic);
			ZkUtils zkUtils = ZkUtils.apply(ZK_CONNECTION, SESSION_TIMEOUT, CONNECTION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			AdminUtils.createTopic(zkUtils, topic, DEFAULT_PARTITION_NUM, DEFAULT_REPLICATION_NUM,
					new Properties(), RackAwareMode.Enforced$.MODULE$);
			zkUtils.close();
		} else {
			LOGGER.info("[*] topic {} 已存在", topic);
		}
	}
	
	/**
	 * 创建topic，并设定分区和副本数
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
			ZkUtils zkUtils = ZkUtils.apply(ZK_CONNECTION, SESSION_TIMEOUT, CONNECTION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
			AdminUtils.createTopic(zkUtils, topic, partition, replication,
					new Properties(), RackAwareMode.Enforced$.MODULE$);
			zkUtils.close();
		} else {
			LOGGER.info("[*] topic {} 已存在", topic);
		}
	}
	
	
	public static void main(String[] args) {
		//createTopic("test0817", 3, 1);
		
		/*Set<String> topics = getAllTopics();
		topics.forEach(v -> System.out.println(v));*/
	}
}
