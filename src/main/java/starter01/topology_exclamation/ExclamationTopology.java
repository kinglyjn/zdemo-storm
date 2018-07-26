package starter01.topology_exclamation;

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

/**
 * ExclamationTopology
 * @author kinglyjn
 * @date 2018年7月24日
 *
 */
public class ExclamationTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		
        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
        conf.setDebug(true);
        conf.setNumWorkers(3);

        String topologyName = "test01-topology";
        if (args != null && args.length > 0) {
            topologyName = args[0];
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
        	LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, conf, builder.createTopology());
        }
	}
	
	
	/**
	 * ExclamationBolt
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class ExclamationBolt extends BaseRichBolt {
		private static final long serialVersionUID = 1L;
		private OutputCollector _collector;
		
		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}	
	}
}
