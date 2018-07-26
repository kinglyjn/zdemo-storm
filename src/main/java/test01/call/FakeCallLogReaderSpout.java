package test01.call;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import utils.NCUtil;


public class FakeCallLogReaderSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	//收集器
	private SpoutOutputCollector collector;
	//完成标记
	private boolean completed = false;
	//storm的上下文信息
	private TopologyContext context;
	private Random randomGenerator = new Random();
	private Integer idx = 0;

	public FakeCallLogReaderSpout() {
		NCUtil.write2NC(this, "call FakeCallLogReaderSpout constructor");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
		NCUtil.write2NC(this, "call open");
	}

	@Override
	public void nextTuple() {
		if (this.idx <= 20) {
			List<String> mobileNumbers = new ArrayList<String>();
			mobileNumbers.add("1234123401");
			mobileNumbers.add("1234123402");
			mobileNumbers.add("1234123403");
			mobileNumbers.add("1234123404");
			Integer localIdx = 0;
			while (localIdx++ < 20 && this.idx++ < 20) {
				//主叫
				String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				//被叫
				String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				while (fromMobileNumber == toMobileNumber) {
					toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				}
				//通话时长
				Integer duration = randomGenerator.nextInt(60);
				//输出通话记录
				this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration), fromMobileNumber + "," + toMobileNumber);
				NCUtil.write2NC(this, fromMobileNumber + "-" + toMobileNumber + "-" + duration);
				System.out.println("===my===>" + fromMobileNumber + "-" + toMobileNumber + "-" + duration);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("from", "to", "duration"));
		NCUtil.write2NC(this, "call declareOutputFields");
	}

	@Override
	public void close() {
		NCUtil.write2NC(this, "call close");
	}

	public boolean isDistributed() {
		return false;
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(Object msgId) {
		NCUtil.write2NC(this, "call ack, msgid=" + msgId);
	}

	@Override
	public void fail(Object msgId) {
		NCUtil.write2NC(this, "call fail, msgid=" + msgId);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
