package test01.call;

import java.util.HashMap;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App {

	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.setNumWorkers(2); //把一个工作进程绑定到topology
		config.setNumAckers(8);
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout(), 1).setNumTasks(1); //一般只能有一个spout分发消息
		builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt(), 3).setNumTasks(6).shuffleGrouping("call-log-reader-spout"); //线程并行度3，任务实例数6，则每个线程执行两个任务实例
		builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt(), 1).setNumTasks(1).fieldsGrouping("call-log-creator-bolt", new Fields("call"));
		
		
		//通过是否有参数来控制是否启动集群（或本地方式运行）
		if(args!=null && args.length>0) {
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			HashMap<String, String> env = new HashMap<String, String>();
			env.put("storm.zookeeper.servers", "nimbusz");
			config.setEnvironment(env);
			config.setDebug(true);
			config.setMaxTaskParallelism(1);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
			
			//本地集群需要手动关闭
			Thread.sleep(10000);
			localCluster.killTopology("LogAnalyserStorm");
			localCluster.shutdown();
		}
	}
}

