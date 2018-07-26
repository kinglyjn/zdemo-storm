package starter03.topology_drpc_reach;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import utils.NCUtil;

/**
 * 主要的功能是统计一下帖子(URL)被二次转发的人数：<br>
 * @see <a href= "http://storm.apache.org/documentation/Distributed-RPC.html">Distributed RPC</a><br>
 * 
 * 实现步骤如下:
 * 第一：获取当前转发帖子的人。 GetTweetersBolt
 * 第二：获取当前人的粉丝(关注者)。GetFollowersBolt
 * 第三：进行粉丝去重。PartialUniquerBatchBolt
 * 第四：统计人数。CountAggregatorBatchBlot
 * 第五：最后使用DRPC远程调用 Topology返回执行结果 TiwtterReachTopology
 * 
 */
@SuppressWarnings("deprecation")
public class ReachTopology {
	// 定义一个数据库（使用map实现）
	private static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
		private static final long serialVersionUID = 1L;
		{
			put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
			put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
			put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
		}
	};
	
	// 定义一个数据库（使用map实现）
	private static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
		private static final long serialVersionUID = 1L;
		{
			put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
	        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
	        put("tim", Arrays.asList("alex"));
	        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
	        put("adam", Arrays.asList("david", "carissa"));
	        put("mike", Arrays.asList("john", "bob"));
	        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
		}
	};
	
	
	/**
	 * 测试说明一：
	 * 服务器端配置 drpc.servers 可以配置多个 drpc 服务主机用于不同拓扑应用的远程调用，默认通信端口为 3772（可以通过 drpc.port 修改）
	 * 客户端 drpc 只能连一台 drpc 主机，这个主机的 drpc 为这个客户端提供 drpc 调用服务
	 * 
	 * 测试说明二：
	 * 创建 LinearDRPCTopologyBuilder 的时候，需要提供一个 DRPC 函数的名称作为输入参数，例如这里的 getReachCount。
	 * 一个 DRPC 服务器可以协调很多函数，函数与函数之间通过函数名称来区分。第一个 Bolt 声明会接收一个2维的的元组，第一个字段是请
	 * 求的id，第二个字段是请求的参数。LinearDRPCTopologyBuilder 期望 topology 的zUI后一个 bolt 发射一个包含[id, result]
	 * 格式的2维元组，第一个字段是请求的id，第二个字段是请求的结果。最终，所有中间元组都必须包含请求的 id 作为它的第一个字段。
	 *
	 * 测试说明三：
	 * 调用远程DRPC服务首先需要将拓扑 jar 上传到远程服务器，配置远程服务器的 drpc.servers 开启相应的drpc服务，执行拓扑任务
	 * 
	 */
	public static void main(String[] args) throws Exception {
		//Trident subsumes the functionality provided by this class, so it's deprecated
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("getReachCount");
        Config conf = new Config();
        
        builder.addBolt(new GetTweeters(), 4);
        builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));
        conf.setDebug(false);
        conf.setNumWorkers(6);
        conf.setMaxTaskParallelism(3);
        
        String topoName = "reach-drpc";
        if (args == null || args.length == 0) {
        	// new drpc & submit topo
        	LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topoName, conf, builder.createLocalTopology(drpc));

            // drpc local invoke test
            String[] urlsToTry = new String[]{ "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" }; // 16 14 0
            for (String url : urlsToTry) {
            	String result = drpc.execute("getReachCount", url);
            	System.err.println(url + " reach count=" + result);
            	NCUtil.write2NC(ReachTopology.class, url + " reach count=" + result);
            }
            
            // shutdown cluster & drpc
            cluster.shutdown();
            drpc.shutdown();
        } else {
        	StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createRemoteTopology());
        }
    }
	
	/**
	 * 客户端测试
	 * 这里使用 config 的话，很多默认的配置信息没有设置上，所以需要从默认的配置中读取才正确
	 * 
	 */
	@Test
	public void testRemoteDrpc() throws Exception {
		Map<String, Object> defaultConfig = Utils.readDefaultConfig(); 
		DRPCClient client = new DRPCClient(defaultConfig, "localhost", 3772);
		String result = client.execute("getReachCount", "foo.com/blog/1");
		System.err.println(client.getHost() + ": " + result);
		client.close();
	}
	
	
	/**
	 * GetTweeters
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class GetTweeters extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;
		
		@Override
	    public void execute(Tuple tuple, BasicOutputCollector collector) {
			// id {Long类型的数，e.g.2412036041250381651}，对于一次drpc请求该值在计算topo中是唯一的
			Object id = tuple.getValue(0);
			NCUtil.write2NC(this, "requestId=" + id);
			// other params
			String url = tuple.getString(1);
			List<String> tweeters = TWEETERS_DB.get(url);
			if (tweeters != null) {
				for (String tweeter : tweeters) {
					collector.emit(new Values(id, tweeter));
		        }
			}
		}
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    	declarer.declare(new Fields("id", "tweeter"));
	    }
	}
	
	/**
	 * GetFollowers
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class GetFollowers extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;

		@Override
	    public void execute(Tuple tuple, BasicOutputCollector collector) {
			// id
			Object id = tuple.getValue(0);
			NCUtil.write2NC(this, "requestId=" + id);
			// other params
			String tweeter = tuple.getString(1);
			List<String> followers = FOLLOWERS_DB.get(tweeter);
			if (followers != null) {
				for (String follower : followers) {
					collector.emit(new Values(id, follower));
				}
			}
		}

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    	declarer.declare(new Fields("id", "follower"));
	    }
	}
	
	/**
	 * PartialUniquer
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class PartialUniquer extends BaseBatchBolt<Object> {
		private static final long serialVersionUID = 1L;
		private BatchOutputCollector _collector;
		private Object _id;
		private Set<String> _followers = new HashSet<String>();

	    @Override
	    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
	    	_collector = collector;
	    	_id = id; // IBatchBolt使用上游节点的第一个field值作为id值
	    }

	    @Override
	    public void execute(Tuple tuple) {
	    	_followers.add(tuple.getString(1));
	    }

	    @Override
	    public void finishBatch() {
	    	_collector.emit(new Values(_id, _followers.size()));
	    	NCUtil.write2NC(this, "requestId=" + _id);
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    	declarer.declare(new Fields("id", "partial-count"));
	    }
	}
	
	/**
	 * CountAggregator
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class CountAggregator extends BaseBatchBolt<Object> {
		private static final long serialVersionUID = 1L;
		private BatchOutputCollector _collector;
		private Object _id;
		private int _count = 0;

	    @Override
	    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
	    	_collector = collector;
	    	_id = id; // IBatchBolt使用上游节点的第一个field值作为id值
	    }

	    @Override
	    public void execute(Tuple tuple) {
	    	_count += tuple.getInteger(1);
	    }

	    @Override
	    public void finishBatch() {
	    	_collector.emit(new Values(_id, _count)); //
	    	NCUtil.write2NC(this, "requestId=" + _id);
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    	declarer.declare(new Fields("id", "reach"));
	    }
	}
}







