package test03.kafka01;

import java.util.HashMap;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import test00.config.ApplicationLoanConfig;

public class ApplicationLoanApp {
	
	public static void main(String[] args) throws InterruptedException {
		Config config = new Config(); 
		config.setNumAckers(ApplicationLoanConfig.TOPOLOGY_ACKER_EXECUTORS);
		TopologyBuilder builder = new TopologyBuilder();
		
		// 设置组件
		builder.setSpout(ApplicationLoanConfig.SPOUT_NAME, new ApplicationLoanSpout(), ApplicationLoanConfig.SPOUT_PARALLELISM)
			.setNumTasks(ApplicationLoanConfig.SPOUT_TASKNUM);
		builder.setBolt(ApplicationLoanConfig.COUNT_BOLT_NAME, new ApplicationLoanCountBolt(), ApplicationLoanConfig.COUNT_BOLT_PARALLELISM)
			.setNumTasks(ApplicationLoanConfig.COUNT_BOLT_TASKNUM)
			.fieldsGrouping(ApplicationLoanConfig.SPOUT_NAME, new Fields("countField"));
		builder.setBolt(ApplicationLoanConfig.SINK_BOLT_NAME, new ApplicationLoanSinkBolt(), ApplicationLoanConfig.SINK_BOLT_PARALLELISM)
			.setNumTasks(ApplicationLoanConfig.SINK_BOLT_TASKNUM)
			.shuffleGrouping(ApplicationLoanConfig.COUNT_BOLT_NAME);
		
		
		if(args!=null && args.length>0) {
			config.setNumWorkers(ApplicationLoanConfig.TOPOLOGY_WORKERS);
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			HashMap<String, String> env = new HashMap<String, String>();
			env.put("storm.zookeeper.servers", ApplicationLoanConfig.ZOOKEEPER_SERVERS);
			config.setEnvironment(env);
			config.setMessageTimeoutSecs(1); //////////
			config.setDebug(true);
			config.setMaxTaskParallelism(1);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(ApplicationLoanConfig.TOPOLOGY_NAME, config, builder.createTopology());
			
			//本地集群需要手动关闭
			Thread.sleep(1000000);
			localCluster.killTopology(ApplicationLoanConfig.TOPOLOGY_NAME);
			localCluster.shutdown();
		}
	}
}

