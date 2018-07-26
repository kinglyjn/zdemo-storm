package test06.transactional_topo01;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.transactional.TransactionalTopologyBuilder;

/**
 * MyTxTopology
 * @author kinglyjn
 * @date 2018年7月26日
 *
 */
@SuppressWarnings("deprecation")
public class MyTxTopology {
	 /*
	 * main
	 * 
	 */
	public static void main(String[] args) throws Exception {
		TransactionalTopologyBuilder txBuilder = new TransactionalTopologyBuilder("MyTxTopo", "MyLogTxSpout", new MyLogTxSpout(), 2);
		Config config = new Config();
		txBuilder.setBolt("MyLogCountTxBolt", new MyLogCountTxBolt(), 3).shuffleGrouping("MyLogTxSpout");
		txBuilder.setBolt("MyLogTxCommitter", new MyLogTxCommitter(), 1).globalGrouping("MyLogCountTxBolt");
		config.setDebug(true);
		config.setNumWorkers(1);
		
		String topologyName = "MyTxTopo";
		if (args!=null && args.length!=0) {
			StormSubmitter.submitTopology(args[0], config, txBuilder.buildTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, config, txBuilder.buildTopology());
		}
	}
}
