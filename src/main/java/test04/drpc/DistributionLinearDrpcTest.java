package test04.drpc;

import java.util.Map;
import org.apache.storm.Config;
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
 * 分布式 DRPC 测试
 * 使用 LinearDRPCTopologyBuilder 构建
 * 
 * 注意：
 * 服务器端配置 drpc.servers 可以配置多个 drpc 服务主机用于不同拓扑应用的远程调用，默认通信端口为 3772（可以通过 drpc.port 修改）
 * 客户端 drpc 只能连一台 drpc 主机，这个主机的 drpc 为这个客户端提供 drpc 调用服务
 *
 */
@SuppressWarnings("deprecation")
public class DistributionLinearDrpcTest {
	private static final String DRPC_HOST = "192.168.1.84";
	private static final Integer DRPC_PORT = 3772;
	private static final String DRPC_FUNC_NAME = "addExclamationMethod";
	
	
	static class ExclaimBolt2 extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			Long requestId = input.getLong(0);
			String requestParam = input.getString(1);
			collector.emit(new Values(requestId, requestParam+"!"));
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}
	}
	
	/**
	 * 启动远程拓扑任务
	 * 首先需要将拓扑 jar 上传到远程服务器，配置远程服务器的 drpc.servers 并开启相应的 drpc 服务
	 * 
	 */
	public static void main(String[] args) throws Exception {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(DRPC_FUNC_NAME);
		builder.addBolt(new ExclaimBolt2(), 3);
		
		if (args!=null && args.length>0) {
			Config config = new Config();
			config.setNumWorkers(3);
			config.setDebug(false);
			StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());
		}
	}
	
	
	/**
	 * 客户端测试
	 * 这里使用 config 的话，很多默认的配置信息没有设置上，所以需要从默认的配置中读取才正确
	 * 
	 */
	@Test
	public void test01() throws Exception {
		Map<String, Object> defaultConfig = Utils.readDefaultConfig(); 
		DRPCClient client = new DRPCClient(defaultConfig, DRPC_HOST, DRPC_PORT);
		String result = client.execute(DRPC_FUNC_NAME, "hello world");
		System.out.println(client.getHost() + ": " + result);
		client.close();
	}
}
