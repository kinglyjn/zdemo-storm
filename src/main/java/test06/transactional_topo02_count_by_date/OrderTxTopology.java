package test06.transactional_topo02_count_by_date;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.transactional.TransactionalTopologyBuilder;

/**
 * OrderTxTopology
 * @author kinglyjn
 * @date 2018年7月26日
 *
 */
@SuppressWarnings("deprecation")
public class OrderTxTopology {
	 /*
	 * main
	 * 
	 */
	public static void main(String[] args) throws Exception {
		TransactionalTopologyBuilder txBuilder = new TransactionalTopologyBuilder("OrderCountByDateTxTopo", "OrderSpout", new OrderSpout(), 1);
		Config config = new Config();
		txBuilder.setBolt("OrderCountBolt", new OrderCountBolt(), 3).shuffleGrouping("OrderSpout");
		txBuilder.setBolt("OrderCommitter", new OrderCommitter(), 1).globalGrouping("OrderCountBolt");
		
		config.setDebug(false);
		config.setNumWorkers(1);
		
		String topologyName = "OrderCountByDateTxTopo";
		if (args!=null && args.length!=0) {
			StormSubmitter.submitTopology(args[0], config, txBuilder.buildTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, config, txBuilder.buildTopology());
		}
	}
}
