package test01.call;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import utils.NCUtil;


public class CallLogCounterBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	//计数器map
	Map<String, Integer> counterMap;
	//收集器
	private OutputCollector collector;
	
	public CallLogCounterBolt() {
		NCUtil.write2NC(this, "call CallLogCounterBolt constructor");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
		NCUtil.write2NC(this, "call prepare");
	}

	@Override
	public void execute(Tuple tuple) {
		//
		String call = tuple.getString(0);
		//
		Integer duration = tuple.getInteger(1);
		if (!counterMap.containsKey(call)) {
			counterMap.put(call, 1);
		} else {
			Integer c = counterMap.get(call) + 1;
			counterMap.put(call, c);
		}
		
		if (new Random().nextBoolean()) {
			collector.ack(tuple);
		} else {
			collector.fail(tuple);
		}
	}

	@Override
	public void cleanup() {
		NCUtil.write2NC(this, "call cleanup");
		for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
			NCUtil.write2NC(this, entry.getKey() + " : " + entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
