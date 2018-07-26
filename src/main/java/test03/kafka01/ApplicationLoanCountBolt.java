package test03.kafka01;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * ApplicationLoanCountBolt
 * @author zhangqingli
 *
 */
public class ApplicationLoanCountBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Map<String, String> baseCountMap;
	

	private void initBaseCountMap() {
		// 查询今天对应的 baseCountMap
		ApplicationLoanRedisService redisRequestLoanService = new ApplicationLoanRedisService();
		baseCountMap = redisRequestLoanService.getTodayBaseCountMap();
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initBaseCountMap();
	}
	
	
	@Override
	public void execute(Tuple input) {
		// apply:20180109:pipeline:AndroidA:12
		// loan:20180109:pipeline:AndroidA:12
		// loan:20180109:levelid:A:12
		String countField = input.getString(0);
		String count = baseCountMap.get(countField);
		if (StringUtils.isEmpty(count)) {
			count = "1";
		} else {
			count = new BigDecimal(count).add(new BigDecimal("1")).toString();
		}
		baseCountMap.put(countField, count);
		
		collector.emit(input, new Values(countField, count));
		collector.ack(input);
	}
	
	
	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("countField", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
