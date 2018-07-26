package starter06.topology_basic_drpc;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import org.junit.Test;

/**
 * BasicDRPCTopology
 * @author kinglyjn
 * @date 2018年7月25日
 *
 */
@SuppressWarnings("deprecation")
public class BasicDRPCTopology {
	public static void main(String[] args) throws Exception {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("getExclamationResult");
		Config config = new Config();
		
		// set components
		builder.addBolt(new ExclaimBolt(), 3);
		
		// set config
		config.setDebug(true);
		//config.setNumWorkers(3);
		
		String topologyName = "BasicDRPCTopology";
		if (args!=null && args.length!=0) {
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createRemoteTopology());
		} else {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, config, builder.createLocalTopology(drpc));
			
			String result = drpc.execute("getExclamationResult", "hellowordxxx");
			System.err.println("result=" + result);
			
			drpc.shutdown();
			cluster.shutdown();
		}
	}
	
	/**
	 * 测试 远程drpc服务
	 * 
	 */
	@Test
	public void testRemoteDRPCService() throws Exception {
		Map<String, Object> defaultConfig = Utils.readDefaultConfig();
		DRPCClient client = new DRPCClient(defaultConfig, "192.168.1.84", 3772); // drpc.port:3772 default
		
		String result = client.execute("getExclamationResult", "aaaaaaaaaaaaaaaaaaa");
		System.out.println("result=" + result);
		
		client.close();
	}
	
	
	/**
	 * ExclaimBolt
	 * @author kinglyjn
	 * @date 2018年7月25日
	 *
	 */
	static class ExclaimBolt extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			Object requestId = input.getValue(0);
			String str = input.getString(1);
			collector.emit(new Values(requestId, str+"!"));
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}
	}
}
