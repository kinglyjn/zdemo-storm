package test02.ack;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import utils.NCUtil;


public class MySpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	//收集器
	private SpoutOutputCollector collector;
	//storm的上下文信息
	private TopologyContext context;
	private Map<Integer, String> toSend = new ConcurrentHashMap<>();
	private Map<Integer, String> allMsg = new ConcurrentHashMap<>();
	private Map<Integer, Integer> failMsg = new ConcurrentHashMap<>();
	private static final Integer MAX_RETRY = 5;
	
	public MySpout() {
		super();
		NCUtil.write2NC(this, "call MySpout constructor");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
		NCUtil.write2NC(this, "call open");
		
		for (int i = 0; i < 10; i++) {
			toSend.put(i, "" + i + ",tom"+i + "," + (10+i));
			allMsg.put(i, "" + i + ",tom"+i + "," + (10+i));
		}
	}

	@Override
	public void nextTuple() {
		if (toSend!=null && !toSend.isEmpty()) {
			for (Map.Entry<Integer, String> entry : toSend.entrySet()) {
				//发送消息
				collector.emit(new Values(entry.getValue()), entry.getKey());
			}
			toSend.clear();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
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
		Integer mid = (Integer) msgId;
		toSend.remove(mid);
		failMsg.remove(mid);
	}

	@Override
	public void fail(Object msgId) {
		NCUtil.write2NC(this, "call fail, msgid=" + msgId);
		Integer mid = (Integer) msgId;
		Integer count = failMsg.get(mid);
		count = count==null ? 0:count;
		if (count<MAX_RETRY) { //消息失败的次数
			failMsg.put(mid, ++count);
			toSend.put(mid, allMsg.get(mid));
		} else {
			NCUtil.write2NC(this, "已经超过最大重试次数，msgid=" + mid);
			toSend.remove(mid);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}




/*
//throw exception 的情况：
[2018-03-18 06:11:08.705,6274@nimbusz,Thread-1,test01.ack.Bolt01_01:1706234378]  call declareOutputFields
[2018-03-18 06:11:09.217,6274@nimbusz,Thread-1,test01.ack.Bolt01_02:371800738]  call declareOutputFields
[2018-03-18 06:11:09.218,6274@nimbusz,Thread-1,test01.ack.Spout01_01:966739377]  call declareOutputFields
[2018-03-18 06:11:21.264,4614@supervisor02z,Thread-41,test01.ack.Bolt01_01:1320857933]  call prepare
[2018-03-18 06:11:21.576,4638@supervisor02z,Thread-45,test01.ack.Bolt01_01:1874248723]  call prepare
[2018-03-18 06:11:21.570,4638@supervisor02z,Thread-39,test01.ack.Bolt01_02:198524870]  call prepare
[2018-03-18 06:11:26.502,5385@supervisor01z,Thread-45,test01.ack.Bolt01_01:1150419429]  call prepare
[2018-03-18 06:11:26.523,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  call open
[2018-03-18 06:11:26.537,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  call activate
[2018-03-18 06:11:26.625,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  call fail, and msgId=c83ed170-4f0e-4ad5-baa6-cfd7b4e205c4
[2018-03-18 06:11:37.659,4758@supervisor02z,Thread-39,test01.ack.Bolt01_02:2006092563]  call prepare
[2018-03-18 06:11:37.688,4758@supervisor02z,Thread-45,test01.ack.Bolt01_01:206052488]  call prepare
[2018-03-18 06:12:15.560,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  call fail, and msgId=c83ed170-4f0e-4ad5-baa6-cfd7b4e205c4
[2018-03-18 06:12:15.588,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  call fail, and msgId=c83ed170-4f0e-4ad5-baa6-cfd7b4e205c4
[2018-03-18 06:12:30.14,4838@supervisor02z,Thread-45,test01.ack.Bolt01_01:1816559784]  call prepare
[2018-03-18 06:12:30.14,4838@supervisor02z,Thread-39,test01.ack.Bolt01_02:1717718824]  call prepare
[2018-03-18 06:13:15.560,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  call fail, and msgId=c83ed170-4f0e-4ad5-baa6-cfd7b4e205c4
[2018-03-18 06:13:15.562,5385@supervisor01z,Thread-39,test01.ack.Spout01_01:1200977203]  已经超过最大重试次数，msgid=c83ed170-4f0e-4ad5-baa6-cfd7b4e205c4

//捕捉但不抛出异常的情况：
[2018-03-18 06:18:33.286,6478@nimbusz,Thread-1,test01.ack.Bolt01_01:1706234378]  call declareOutputFields
[2018-03-18 06:18:33.799,6478@nimbusz,Thread-1,test01.ack.Bolt01_02:371800738]  call declareOutputFields
[2018-03-18 06:18:33.804,6478@nimbusz,Thread-1,test01.ack.Spout01_01:966739377]  call declareOutputFields
[2018-03-18 06:18:53.713,5023@supervisor02z,Thread-41,test01.ack.Bolt01_01:1490884015]  call prepare
[2018-03-18 06:18:54.123,5044@supervisor02z,Thread-45,test01.ack.Bolt01_01:961730417]  call prepare
[2018-03-18 06:18:54.148,5044@supervisor02z,Thread-39,test01.ack.Bolt01_02:1003126523]  call prepare
[2018-03-18 06:19:02.925,5567@supervisor01z,Thread-45,test01.ack.Bolt01_01:59098438]  call prepare
[2018-03-18 06:19:02.923,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  call open
[2018-03-18 06:19:02.946,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  call activate
[2018-03-18 06:19:02.988,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  call fail, and msgId=373876c7-2082-41d7-84b6-32fab7e116fa
[2018-03-18 06:19:03.6,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  call fail, and msgId=373876c7-2082-41d7-84b6-32fab7e116fa
[2018-03-18 06:19:03.22,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  call fail, and msgId=373876c7-2082-41d7-84b6-32fab7e116fa
[2018-03-18 06:19:03.42,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  call fail, and msgId=373876c7-2082-41d7-84b6-32fab7e116fa
[2018-03-18 06:19:03.43,5567@supervisor01z,Thread-39,test01.ack.Spout01_01:1057659493]  已经超过最大重试次数，msgid=373876c7-2082-41d7-84b6-32fab7e116fa

主要的区别在于：
异常直接抛出之后，该异常bolt任务直接挂掉，这个挂掉的任务（task实例）连同其相关tuple树相关任务实例又会在某个节点重启，以执行fail方法重新发送消息
而异常直接抛出并捕捉之后，该异常bolt任务不会挂掉(执行速度较快，不用反序列化重启任务)，fail方法重新发送消息，但是storm ui 界面看不到相关的异常信息，只能通过日志查看异常信息。

*/
