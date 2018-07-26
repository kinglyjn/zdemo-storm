package test02.ack;

import java.util.HashMap;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class App {

	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.setNumAckers(8);
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("myspout", new MySpout(), 1); //并行度表着一个组件的初始 executor （也是线程）数量
		builder.setBolt("mybolt", new MyBolt(), 3).setNumTasks(3).shuffleGrouping("myspout");
		builder.setBolt("mybolt2", new MyBolt2(), 1).shuffleGrouping("mybolt");
		
		//通过是否有参数来控制是否启动集群（或本地方式运行）
		if(args!=null && args.length>0) {
			//最好一台机器上的一个topology只使用一个worker,主要原因时减少了worker之间的数据传输
			config.setNumWorkers(1);
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			HashMap<String, String> env = new HashMap<String, String>();
			env.put("storm.zookeeper.servers", "nimbusz");
			config.setEnvironment(env);
			config.setMessageTimeoutSecs(1); //////////
			config.setDebug(true);
			config.setMaxTaskParallelism(1);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("acktest", config, builder.createTopology());
			
			//本地集群需要手动关闭
			Thread.sleep(10000);
			localCluster.killTopology("acktest");
			localCluster.shutdown();
		}
	}
}

