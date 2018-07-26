package starter05.topology_rolling_top_words2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.RollingCountBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * RollingTopWordsTopology
 * @author kinglyjn
 * @date 2018年7月24日
 *
 */
public class RollingTopWordsTopology {
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;
	
	 /*
	 * main
	 * 
	 */
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		
	    builder.setSpout("wordGenerator", new TestWordSpout(), 5);
	    builder.setBolt("counter", new RollingCountBolt(9, 3), 4).fieldsGrouping("wordGenerator", new Fields("word"));
	    builder.setBolt("intermediateRanker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("counter", new Fields("obj"));
	    builder.setBolt("finalRanker", new TotalRankingsBolt(TOP_N)).globalGrouping("intermediateRanker");
	    conf.setDebug(true);
	    conf.setNumWorkers(3);
	    
	    String topologyName = "TopWordsTopology";
	    if (args!=null && args.length!=0) {
	    	StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    } else {
	    	LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology(topologyName, conf, builder.createTopology());
	       
	        Thread.sleep(DEFAULT_RUNTIME_IN_SECONDS*1000);
	        cluster.killTopology(topologyName);
	        cluster.shutdown();
	    }
	}
	
}
