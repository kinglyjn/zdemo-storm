package trident03.hbase_integration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;


public class TopoApp {
	
	 /*
	 * main
	 * 
	 */
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config config = new Config();
		config.setNumWorkers(3);
		config.setDebug(false);
		
		// set components
		builder.setSpout("orderSpout", new OrderSpout(), 3);
		
		
		// start topo tasks
		String topoName = "orderTopoApp";
		if (args==null || args.length==0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topoName, config, builder.createTopology());
		} else {
			StormSubmitter.submitTopology(topoName, config, builder.createTopology());
		}
	}
}
