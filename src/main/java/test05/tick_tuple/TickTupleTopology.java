package test05.tick_tuple;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

/**
 * Storm 0.8.0以后中内置了一种定时机制——tick，它能够让任何bolt的所有task每隔一段时间（精确到秒级，用户可以自定义）
 * 收到一个来自__systemd的__tick stream的tick tuple，bolt收到这样的tuple后可以根据业务需求完成相应的处理。
 * 
 * 若希望某个bolt每隔一段时间做一些操作，那么可以将bolt继承BaseBasicBolt/BaseRichBolt，并重写getComponentConfiguration()方法。
 * 在方法中设置Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS的值，单位是秒。这样设置之后，此bolt的所有task都会每隔一段时间收到一个来自
 * __systemd的__tick stream的tick tuple。
 * 
 * 若希望Topology中的每个bolt都每隔一段时间做一些操作，那么可以定义一个Topology全局的tick，同样是设置
 * Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS的值
 * 
 * Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS是精确到秒级的。例如某bolt设置Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS为10s，
 * 理论上说bolt的每个task应该每个10s收到一个tick tuple。实际测试发现，这个时间间隔的精确性是很高的，一般延迟（而不是提前）
 * 时间在1ms左右。测试环境：3台虚拟机做supervisor，每台配置：4Cpu、16G内存、千兆网卡。
 * 
 * TickTupleTopology
 * @author kinglyjn
 * @date 2018年7月25日
 *
 */
public class TickTupleTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		
		// set executors
		builder.setSpout("wordSpout", new TestWordSpout() {
			private static final long serialVersionUID = 1L;
			@Override
			public void nextTuple() {
				super.nextTuple();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}, 1);
		builder.setBolt("addingExclamationBolt1", new AddingExclamationBolt1(), 5).shuffleGrouping("wordSpout");
		builder.setBolt("addingExclamationBolt2", new AddingExclamationBolt2(), 3).shuffleGrouping("addingExclamationBolt1");
		
		// set conf
		conf.setDebug(false);
		conf.setNumWorkers(3);
		
		String topologyName = "TickTupleTopo";
		if (args!=null && args.length!=0) {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
			
			Thread.sleep(Integer.MAX_VALUE);
			cluster.shutdown();
		}
	}
	
	/**
	 * AddingExclamationBolt1
	 * @author kinglyjn
	 * @date 2018年7月25日
	 *
	 */
	static class AddingExclamationBolt1 extends BaseRichBolt {
		private static final long serialVersionUID = 1L;
		private OutputCollector _collector;
		
		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			if (TupleUtils.isTick(tuple)) { 
				System.err.println(Thread.currentThread().getName() + ": tick tuple triggered, do sth...");
				System.err.println(tuple.hashCode() + ": " + tuple.getValues()); // [10]
			} else {
				_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
				_collector.ack(tuple);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			Map<String, Object> conf = new HashMap<String, Object>();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
			return conf;
		}
	}
	
	/**
	 * AddingExclamationBolt2
	 * @author kinglyjn
	 * @date 2018年7月25日
	 *
	 */
	static class AddingExclamationBolt2 extends BaseRichBolt {
		private static final long serialVersionUID = 1L;
		private OutputCollector _collector;
		
		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			if (TupleUtils.isTick(tuple)) {
				System.err.println(Thread.currentThread().getName() + ": tick tuple triggered, do sth...");
				System.err.println(tuple.hashCode() + ": " + tuple.getValues()); // [15]
			} else {
				_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
				System.out.println(Thread.currentThread().getName() + ": " + tuple.getValues());
				_collector.ack(tuple);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			Map<String, Object> conf = new HashMap<String, Object>();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 15);
			return conf;
		}
	}
}
