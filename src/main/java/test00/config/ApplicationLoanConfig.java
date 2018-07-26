package test00.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * ApplicationLoanConfig
 *
 */
public class ApplicationLoanConfig {
	public static final String DEFAULT_SEPARATOR = ":";
	public static final String TYPE_APPLY = "apply";
	public static final String TYPE_LOAN = "loan";
	public static final String COLUMN_PIPELINE = "pipeline";
	public static final String COLUMN_LEVELID = "levelid";
	public static final String COUNT_TYPE_APPLY_PIPELINE = "apply_pipeline";
	public static final String COUNT_TYPE_LOAN_PIPELINE = "loan_pipeline";
	public static final String COUNT_TYPE_LOAN_LEVELID = "loan_levelid";
	
	public static String ZOOKEEPER_SERVERS;
	public static Integer ZOOKEEPER_SESSION_TIMEOUT;
	public static Integer ZOOKEEPER_CONNECTION_TIMEOUT;

	public static String KAFKA_ZOOKEEPER_ROOT;
	public static String KAFKA_BOOTSTRAP_SERVERS;
	public static String[] KAFKA_TOPICS;
	public static String KAFKA_CUSTOMER_GROUP;
	public static Boolean KAFKA_CUSTOMER_ENABLE_AUTO_COMMIT;
	public static Integer KAFKA_CUSTOMER_MAX_POLL_RECORDS;
	public static Integer KAFKA_DEFAULT_PATITION;
	public static Integer KAFKA_DEFAULT_REPLICATION;
	
	public static String TOPOLOGY_NAME;
	public static Integer TOPOLOGY_WORKERS;
	public static Integer TOPOLOGY_ACKER_EXECUTORS;
	public static String SPOUT_NAME;
	public static Integer SPOUT_PARALLELISM;
	public static Integer SPOUT_TASKNUM;
	public static Integer SPOUT_MAX_RETRIES;
	public static String COUNT_BOLT_NAME;
	public static Integer COUNT_BOLT_PARALLELISM;
	public static Integer COUNT_BOLT_TASKNUM;
	public static String SINK_BOLT_NAME;
	public static Integer SINK_BOLT_PARALLELISM;
	public static Integer SINK_BOLT_TASKNUM;
	
	public static String REDIS_HOST;
	public static Integer REDIS_PORT;
	public static String REDIS_PASSWD;
	
	
	static {
		Properties props = new Properties();
		InputStream is = ApplicationLoanConfig.class.getClassLoader().getResourceAsStream("storm.properties");
		try {
			props.load(is);
			ZOOKEEPER_SERVERS = props.getProperty("applicationloan.zookeeper.servers");
			ZOOKEEPER_SESSION_TIMEOUT = Integer.parseInt(props.getProperty("applicationloan.zookeeper.session_timeout"));
			ZOOKEEPER_CONNECTION_TIMEOUT = Integer.parseInt(props.getProperty("applicationloan.zookeeper.connection_timeout"));
			
			KAFKA_ZOOKEEPER_ROOT = props.getProperty("applicationloan.kafka.zookeeper.root");
			KAFKA_BOOTSTRAP_SERVERS = props.getProperty("applicationloan.kafka.bootstrap_servers");
			KAFKA_TOPICS = props.getProperty("applicationloan.kafka.topics").split(",");
			KAFKA_CUSTOMER_GROUP = props.getProperty("applicationloan.kafka.customer.group");
			KAFKA_CUSTOMER_ENABLE_AUTO_COMMIT = Boolean.parseBoolean(props.getProperty("applicationloan.kafka.customer.enable_auto_commit"));
			KAFKA_CUSTOMER_MAX_POLL_RECORDS = Integer.parseInt(props.getProperty("applicationloan.kafka.customer.max_poll_records"));
			KAFKA_DEFAULT_PATITION = Integer.parseInt(props.getProperty("applicationloan.kafka.default_patition"));
			KAFKA_DEFAULT_REPLICATION = Integer.parseInt(props.getProperty("applicationloan.kafka.default_replication"));
			
			TOPOLOGY_NAME = props.getProperty("applicationloan.topology.name");
			TOPOLOGY_WORKERS = Integer.parseInt(props.getProperty("applicationloan.topology_workers"));
			TOPOLOGY_ACKER_EXECUTORS = Integer.parseInt(props.getProperty("applicationloan.topology_acker_executors"));
			SPOUT_NAME = props.getProperty("applicationloan.spout.name");
			SPOUT_PARALLELISM = Integer.parseInt(props.getProperty("applicationloan.spout.parallelism"));
			SPOUT_TASKNUM = Integer.parseInt(props.getProperty("applicationloan.spout.tasknum"));
			SPOUT_MAX_RETRIES = Integer.parseInt(props.getProperty("applicationloan.spout.max_retries"));
			COUNT_BOLT_NAME = props.getProperty("applicationloan.count_bolt.name");
			COUNT_BOLT_PARALLELISM = Integer.parseInt(props.getProperty("applicationloan.count_bolt.parallelism"));
			COUNT_BOLT_TASKNUM = Integer.parseInt(props.getProperty("applicationloan.count_bolt.tasknum"));
			SINK_BOLT_NAME = props.getProperty("applicationloan.sink_bolt.name");
			SINK_BOLT_PARALLELISM = Integer.parseInt(props.getProperty("applicationloan.sink_bolt.parallelism"));
			SINK_BOLT_TASKNUM = Integer.parseInt(props.getProperty("applicationloan.sink_bolt.tasknum"));
			
			REDIS_HOST = props.getProperty("applicationloan.redis.host");
			REDIS_PORT = Integer.parseInt(props.getProperty("applicationloan.redis.port"));
			REDIS_PASSWD = props.getProperty("applicationloan.redis.passwd");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
