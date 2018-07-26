package test04.drpc;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import org.junit.Test;

/**
 * 分布式 DRPC 测试
 * 使用 TopologyBuilder 构建拓扑
 * 
 */
public class DistributionLinearDrpcTest2 {
	private static final String DRPC_HOST = "nimbusz";
	private static final Integer DRPC_PORT = 3772;
	private static final String DRPC_FUNC_NAME = "addExclamationMethod";

	static class ExclamationAdditionBolt2 extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String requestParam = input.getString(0);
			Object returnInfo = input.getValue(1); //returnInfo的内容：{"port":0,"host":"1f4464ea-b618-4e5f-a8ab-aaa94ab1ffec","id":"1"}
			collector.emit(new Values(requestParam+"!!", returnInfo));
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("result", "returnInfo"));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args==null || args.length==0) {
			TopologyBuilder builder = new TopologyBuilder();
			LocalDRPC drpc = new LocalDRPC();
			builder.setSpout("exclamationSpout", new DRPCSpout(DRPC_FUNC_NAME, drpc), 3);
			builder.setBolt("exclamationAdditionBolt", new ExclamationAdditionBolt2(), 3).shuffleGrouping("exclamationSpout");
			builder.setBolt("exclamationReturnBolt", new ReturnResults(), 3).shuffleGrouping("exclamationAdditionBolt");
			
			Config config = new Config();
			config.setDebug(false);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("exclamationTopology", config, builder.createTopology());
			
			String result = drpc.execute(DRPC_FUNC_NAME, "hello world");
			System.err.println("result: " + result);
			cluster.shutdown();
			drpc.shutdown();
		} else {
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("exclamationSpout", new DRPCSpout(DRPC_FUNC_NAME), 3);
			builder.setBolt("exclamationAdditionBolt", new ExclamationAdditionBolt2(), 3).shuffleGrouping("exclamationSpout");
			builder.setBolt("exclamationReturnBolt", new ReturnResults(), 3).shuffleGrouping("exclamationAdditionBolt");
			
			Config config = new Config();
			config.setDebug(false);
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		}
	}
	
	
	@Test
	public void test01() throws Exception {
		Map<String, Object> defaultConfig = Utils.readDefaultConfig(); 
		DRPCClient client = new DRPCClient(defaultConfig, DRPC_HOST, DRPC_PORT);
		String result = client.execute(DRPC_FUNC_NAME, "hello world");
		System.out.println(client.getHost() + ": " + result);
		client.close();
	}
}
