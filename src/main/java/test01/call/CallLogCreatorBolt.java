package test01.call;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import utils.NCUtil;

public class CallLogCreatorBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	public CallLogCreatorBolt() {
		super();
		NCUtil.write2NC(this, "call CallLogCreatorBolt constructor");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		NCUtil.write2NC(this, "call prepare");
	}

	@Override
	public void execute(Tuple tuple) {
		//主叫
		String from = tuple.getString(0);
		//被叫
		String to = tuple.getString(1);
		//通话时长
		Integer duration = tuple.getInteger(2);
		//发送，锚定tuple用于ack确认
		collector.emit(tuple, new Values(from + " - " + to, duration));
		NCUtil.write2NC(this, from + "-" + to);
		System.out.println("===my===>" + from + "-" + to);
		collector.ack(tuple); //中间每一步都需要ack或者fail
	}

	@Override
	public void cleanup() {
		NCUtil.write2NC(this, "call cleanup");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call", "duration"));
		NCUtil.write2NC(this, "call declareOutputFields");
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
