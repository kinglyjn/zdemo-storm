package test03.kafka01;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import test00.config.ApplicationLoanConfig;
import utils.NCUtil;

/**
 * ApplicationLoanSinkBolt
 * @author zhangqingli
 *
 */
public class ApplicationLoanSinkBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationLoanSinkBolt.class);
	public static final Integer REDIS_KEYVALUE_EXPIRE_SECONDS = 691200; //8 day
	private OutputCollector collector;
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	
	@Override
	public void execute(Tuple input) {
		String countField = input.getString(0);
		String count = input.getString(1);
		
		/*
		 * 保留最近8天数据
		 * 申请量 & 放款量
		 * apply:20180109
		 *	   pipeline:AndroidA:12 x
		 * loan:20180109
		 *     pipeline:AndroidA:0  x
		 *	   riskrank:A:21        x
		 */
		try {
			// apply:20180109:pipeline:AndroidA:12
			String[] splits = countField.split(ApplicationLoanConfig.DEFAULT_SEPARATOR);
			if (splits.length != 5) {
				return;
			}
			ApplicationLoanRedisService redisService = new ApplicationLoanRedisService();
			String hkey = splits[0] + ApplicationLoanConfig.DEFAULT_SEPARATOR + splits[1];
			String field = splits[2] + ApplicationLoanConfig.DEFAULT_SEPARATOR + splits[3] + ApplicationLoanConfig.DEFAULT_SEPARATOR + splits[4];
			redisService.hset(hkey, field, count);
			redisService.expire(hkey, REDIS_KEYVALUE_EXPIRE_SECONDS);
			
			collector.ack(input);
		} catch (Exception e) {
			LOGGER.error("ApplicationLoanSinkBolt#execute 出现错误！");
			NCUtil.write2NC(this, "ApplicationLoanSinkBolt#execute 出现错误！");
			collector.fail(input);
		}
	}
	
	public void sinkResultData(String type, Long createTime, String pipelineName, String riskRank) {
		String msg = type + ":" + createTime + ":" + pipelineName + ":" + riskRank;
		NCUtil.write2NC(this, msg);
	}
	

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
