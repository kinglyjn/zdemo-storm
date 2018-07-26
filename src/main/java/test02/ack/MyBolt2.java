package test02.ack;

import java.util.Map;
import java.util.Random;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import utils.NCUtil;

public class MyBolt2 implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	public MyBolt2() {
		super();
		NCUtil.write2NC(this, "call MyBolt2 constructor");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		NCUtil.write2NC(this, "call prepare");
	}

	@Override
	public void execute(Tuple tuple) {
		String message = tuple.getString(0);
		NCUtil.write2NC(this, message);
		
		if (new Random().nextBoolean()) {
			collector.ack(tuple);
			NCUtil.write2NC(this, message + ":ack");
		} else {
			collector.fail(tuple);
			NCUtil.write2NC(this, message + ":fail");
		}
	}

	@Override
	public void cleanup() {
		NCUtil.write2NC(this, "call cleanup");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
