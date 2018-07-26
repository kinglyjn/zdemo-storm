package test02.ack;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import utils.NCUtil;

public class MyBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	public MyBolt() {
		super();
		NCUtil.write2NC(this, "call MyBolt constructor");
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
		collector.emit(tuple, new Values(message));
		
//		try {
//			Thread.sleep(2000); //
//		} catch (Exception e) {}
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
		NCUtil.write2NC(this, "call cleanup");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
