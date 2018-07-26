package test04.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("deprecation")
public class LocalLinearDrpcTest {

	/**
	 * 创建 LinearDRPCTopologyBuilder 的时候，需要提供一个 DRPC 函数的名称作为输入参数，例如这里的 addExclamationMethod。
	 * 一个 DRPC 服务器可以协调很多函数，函数与函数之间通过函数名称来区分。第一个 Bolt 声明会接收一个2维的的元组，第一个字段是请
	 * 求的id，第二个字段是请求的参数。LinearDRPCTopologyBuilder 期望 topology 的zUI后一个 bolt 发射一个包含[id, result]
	 * 格式的2维元组，第一个字段是请求的id，第二个字段是请求的结果。最终，所有中间元组都必须包含请求的 id 作为它的第一个字段。
	 *
	 */
	static class ExclaimBolt1 extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			Long requestId = input.getLong(0);
			String requestParam = input.getString(1);
			collector.emit(new Values(requestId, requestParam+"!"));
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("idhaha", "resulthaha"));
		}
	}
	
	public static void main(String[] args) {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addExclamationMethod");
		builder.addBolt(new ExclaimBolt1(), 3);
		
		Config config = new Config();
		config.setDebug(false);
		
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("exclamationTopology", config, builder.createLocalTopology(drpc));
		
		// test
		String result = drpc.execute("addExclamationMethod", "hello world");
		System.err.println("result: " + result);
		
		cluster.shutdown(); 
		drpc.shutdown();
	}
	
}
