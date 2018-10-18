package trident01.hello;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import com.google.common.collect.ImmutableList;


/**
 * TridentTopologyApp
 * @author kinglyjn
 * @date 2018年8月13日
 *
 */
public class TridentTopologyApp {

	/**
	 * 使用FeederBatchSpout测试 shuffle、function、parallelismHint并发设置
	 * 
	 */
	@Test
	public void test01() throws Exception {
		//
		TridentTopology topology = new TridentTopology();
		Config config = new Config();
		config.setNumWorkers(1);
		config.setDebug(false);
		
		//
		FeederBatchSpout spout = new FeederBatchSpout(ImmutableList.of("a","b","c","d"));
		
		Stream stream = topology.newStream("fixed-batch-spout", spout);
		stream.shuffle().each(new Fields("a","b"), new CheckEvenSumFilter()).parallelismHint(2)
			.each(new Fields("a","b"), new SumFunction(), new Fields("sum")).parallelismHint(3); //并发度设置为3，即executor数为3（默认executor数=task数）
	
		// 
		boolean localMode = true;
		if (localMode) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", config, topology.build());
		} else {
			StormSubmitter.submitTopology("mytopology", config, topology.build());
		}

		//
		spout.feed(ImmutableList.of(
				new Values(1,1,3,4), 
				new Values(3,1,4,1), 
				new Values(2,3,4,1), 
				new Values(2,4,1,2)));
		Thread.sleep(10000);
		
		/*
		thread=Thread-32-b-0-executor[4 4], object=CheckEvenSumFilter@1290957494: num0=2, num1=3, isKeep=false
		thread=Thread-26-b-0-executor[6 6], object=CheckEvenSumFilter@1723964479: num0=1, num1=1, isKeep=true
		thread=Thread-32-b-0-executor[4 4], object=CheckEvenSumFilter@1290957494: num0=2, num1=4, isKeep=true
		thread=Thread-26-b-0-executor[6 6], object=SumFunction@1745251991: num0=1, num1=1, sum=2
		thread=Thread-32-b-0-executor[4 4], object=SumFunction@355431110: num0=2, num1=4, sum=6
		thread=Thread-26-b-0-executor[6 6], object=CheckEvenSumFilter@1723964479: num0=3, num1=1, isKeep=true
		thread=Thread-26-b-0-executor[6 6], object=SumFunction@1745251991: num0=3, num1=1, sum=4
		*/
	}
	
	
	
	
	/**
	 * 使用FixedBatchSpout测试partitionBy、broadcast
	 * 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void test02() throws Exception {
		TridentTopology topology = new TridentTopology();
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3, 
				new Values(1,3), 
				new Values(2,3), 
				new Values(3,2), 
				new Values(4,2));
		spout.setCycle(true); //设置循环发送tuple，每个批次tuple数最大为3
		
		topology.newStream("tx-1", spout)
				.shuffle().each(new Fields("a", "b"), new CheckEvenSumFilter()).parallelismHint(2)
				.shuffle().each(new Fields("a","b"), new CheckEvenFirstFilter()).parallelismHint(3)
				.partitionBy(new Fields("a")).each(new Fields("a","b"), new SumFunction(), new Fields("sum")).parallelismHint(2) //数据发生了倾斜，导致[4,2]这条数据只用SumFunction的一个executor处理
				.broadcast().each(new Fields("a", "b", "sum"), new PrinterFunction(), new Fields()).parallelismHint(3);
		
		boolean localMode = true;
		if (localMode) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("myapp", config, topology.build());
		} else {
			StormSubmitter.submitTopology("myapp", config, topology.build());
		}
		Thread.sleep(10000);
		
		/*
		thread=Thread-40-b-0-executor[5 5], object=CheckEvenSumFilter@1345143111: num0=2, num1=3, isKeep=false
		thread=Thread-46-b-0-executor[4 4], object=CheckEvenSumFilter@1991291959: num0=1, num1=3, isKeep=true
		thread=Thread-46-b-0-executor[4 4], object=CheckEvenSumFilter@1991291959: num0=3, num1=2, isKeep=false
		thread=Thread-18-b-1-executor[8 8], object=CheckEvenFirstFilter@643101710: num0=1, isKeep=false
		thread=Thread-46-b-0-executor[4 4], object=CheckEvenSumFilter@1991291959: num0=4, num1=2, isKeep=true
		thread=Thread-24-b-1-executor[7 7], object=CheckEvenFirstFilter@980721018: num0=4, isKeep=true
		thread=Thread-42-b-2-executor[10 10], object=SumFunction@423246389: num0=4, num1=2, sum=6
		thread=Thread-20-b-3-executor[12 12], object=PrinterFunction@1635168913: num0=4, num1=2, sumResult=6
		thread=Thread-38-b-3-executor[13 13], object=PrinterFunction@81068949: num0=4, num1=2, sumResult=6
		thread=Thread-34-b-3-executor[11 11], object=PrinterFunction@180477650: num0=4, num1=2, sumResult=6
		thread=Thread-40-b-0-executor[5 5], object=CheckEvenSumFilter@1345143111: num0=1, num1=3, isKeep=true
		thread=Thread-46-b-0-executor[4 4], object=CheckEvenSumFilter@1991291959: num0=3, num1=2, isKeep=false
		thread=Thread-40-b-0-executor[5 5], object=CheckEvenSumFilter@1345143111: num0=2, num1=3, isKeep=false
		thread=Thread-18-b-1-executor[8 8], object=CheckEvenFirstFilter@643101710: num0=1, isKeep=false
		thread=Thread-46-b-0-executor[4 4], object=CheckEvenSumFilter@1991291959: num0=4, num1=2, isKeep=true
		thread=Thread-30-b-1-executor[6 6], object=CheckEvenFirstFilter@1818871452: num0=4, isKeep=true
		thread=Thread-42-b-2-executor[10 10], object=SumFunction@423246389: num0=4, num1=2, sum=6
		thread=Thread-34-b-3-executor[11 11], object=PrinterFunction@180477650: num0=4, num1=2, sumResult=6
		thread=Thread-20-b-3-executor[12 12], object=PrinterFunction@1635168913: num0=4, num1=2, sumResult=6
		thread=Thread-38-b-3-executor[13 13], object=PrinterFunction@81068949: num0=4, num1=2, sumResult=6
		*/
	}
	
	
	
	
	/**
	 * 使用FixedBatchSpout测试partition和batch聚合操作
	 * 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void test03() throws Exception {
		TridentTopology topology = new TridentTopology();
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3, 
				new Values(1,3), 
				new Values(2,3), 
				new Values(6,2), 
				new Values(4,2));
		spout.setCycle(true);
		
		topology.newStream("tx-1", spout)
				.shuffle().each(new Fields("a", "b"), new CheckEvenFirstFilter()).parallelismHint(3)
				.shuffle().each(new Fields("a","b"), new SumFunction(), new Fields("sum")).parallelismHint(2)
				//.partitionAggregate(new Fields("a"), new MySumReducerAggregator(), new Fields("aggSum")) // 1
				//.aggregate(new Fields("a"), new MySumAggregator(), new Fields("aggSum")) // 2
				.aggregate(new Fields("a"), new MySumCombinerAggregator(), new Fields("aggSum")) // 3
				.global().each(new Fields("aggSum"), new PrinterFunction(), new Fields());
				
		boolean localMode = true;
		if (localMode) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("myapp", config, topology.build());
		} else {
			StormSubmitter.submitTopology("myapp", config, topology.build());
		}
		Thread.sleep(10000);
		
		/*
		1:
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@187416297: prepare
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@993562915: prepare
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: prepare
		thread=Thread-34-b-0-executor[5 5], object=CheckEvenFirstFilter@442939294: prepare
		thread=Thread-38-b-0-executor[4 4], object=CheckEvenFirstFilter@408387850: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1605324370: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1605324370: num0=1, isKeep=false
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1605324370: num0=2, isKeep=true
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1605324370: num0=6, isKeep=true
		thread=Thread-18-b-2-executor[8 8], object=MySumReducerAggregator@1815109696: init
		thread=Thread-30-b-2-executor[9 9], object=MySumReducerAggregator@1380714911: init
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@993562915: num0=2, num1=3, sum=5
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@187416297: num0=6, num1=2, sum=8
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=6
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=2
		thread=Thread-38-b-0-executor[4 4], object=CheckEvenFirstFilter@408387850: num0=4, isKeep=true
		thread=Thread-18-b-2-executor[8 8], object=MySumReducerAggregator@1815109696: init
		thread=Thread-30-b-2-executor[9 9], object=MySumReducerAggregator@1380714911: init
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@993562915: num0=4, num1=2, sum=6
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=0
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=4
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1605324370: num0=2, isKeep=true
		thread=Thread-34-b-0-executor[5 5], object=CheckEvenFirstFilter@442939294: num0=6, isKeep=true
		thread=Thread-38-b-0-executor[4 4], object=CheckEvenFirstFilter@408387850: num0=1, isKeep=false
		thread=Thread-18-b-2-executor[8 8], object=MySumReducerAggregator@1815109696: init
		thread=Thread-30-b-2-executor[9 9], object=MySumReducerAggregator@1380714911: init
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@187416297: num0=2, num1=3, sum=5
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@187416297: num0=6, num1=2, sum=8
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=8
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=0
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1605324370: num0=4, isKeep=true
		thread=Thread-18-b-2-executor[8 8], object=MySumReducerAggregator@1815109696: init
		thread=Thread-30-b-2-executor[9 9], object=MySumReducerAggregator@1380714911: init
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@187416297: num0=4, num1=2, sum=6
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=4
		thread=Thread-22-b-1-executor[7 7], object=PrinterFunction@395614525: value=0
		
		2:
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@3023498: prepare
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1489844109: prepare
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2126273663: prepare
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1650085194: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@386914937: prepare
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@593499596: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@386914937: num0=1, isKeep=false
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@386914937: num0=2, isKeep=true
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@386914937: num0=6, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@593499596: num0=2, num1=3, sum=5
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1489844109: num0=6, num1=2, sum=8
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@870463031: init, batchIdClass=TransactionAttempt, batchId=1:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@870463031: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1650085194: value=8
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2126273663: num0=4, isKeep=true
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1489844109: num0=4, num1=2, sum=6
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@870463031: init, batchIdClass=TransactionAttempt, batchId=2:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@870463031: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1650085194: value=4
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2126273663: num0=6, isKeep=true
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@3023498: num0=2, isKeep=true
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@386914937: num0=1, isKeep=false
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@593499596: num0=6, num1=2, sum=8
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1489844109: num0=2, num1=3, sum=5
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@870463031: init, batchIdClass=TransactionAttempt, batchId=3:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@870463031: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1650085194: value=8
		
		3:
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2037800273: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@654519276: prepare
		thread=Thread-38-b-3-executor[10 10], object=SumFunction@529949335: prepare
		thread=Thread-18-b-2-executor[8 8], object=PrinterFunction@929426307: prepare
		thread=Thread-30-b-3-executor[9 9], object=SumFunction@1366130949: prepare
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@128773816: prepare
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2037800273: num0=1, isKeep=false
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@128773816: num0=2, isKeep=true
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2037800273: num0=6, isKeep=true
		thread=Thread-38-b-3-executor[10 10], object=MySumCombinerAggregator@290561813: zero
		thread=Thread-30-b-3-executor[9 9], object=MySumCombinerAggregator@1605006680: zero
		thread=Thread-30-b-3-executor[9 9], object=SumFunction@1366130949: num0=6, num1=2, sum=8
		thread=Thread-30-b-3-executor[9 9], object=MySumCombinerAggregator@1605006680: init, num=6
		thread=Thread-30-b-3-executor[9 9], object=MySumCombinerAggregator@1605006680: combine, 0+6=6
		thread=Thread-30-b-3-executor[9 9], object=SumFunction@1366130949: num0=2, num1=3, sum=5
		thread=Thread-30-b-3-executor[9 9], object=MySumCombinerAggregator@1605006680: init, num=2
		thread=Thread-30-b-3-executor[9 9], object=MySumCombinerAggregator@1605006680: combine, 6+2=8
		thread=Thread-22-b-1-executor[7 7], object=MySumCombinerAggregator@1842040246: zero
		thread=Thread-22-b-1-executor[7 7], object=MySumCombinerAggregator@1842040246: combine, 0+0=0
		thread=Thread-22-b-1-executor[7 7], object=MySumCombinerAggregator@1842040246: combine, 0+8=8
		thread=Thread-18-b-2-executor[8 8], object=PrinterFunction@929426307: value=8
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@128773816: num0=4, isKeep=true
		thread=Thread-30-b-3-executor[9 9], object=MySumCombinerAggregator@1605006680: zero
		thread=Thread-38-b-3-executor[10 10], object=MySumCombinerAggregator@290561813: zero
		thread=Thread-38-b-3-executor[10 10], object=SumFunction@529949335: num0=4, num1=2, sum=6
		thread=Thread-38-b-3-executor[10 10], object=MySumCombinerAggregator@290561813: init, num=4
		thread=Thread-38-b-3-executor[10 10], object=MySumCombinerAggregator@290561813: combine, 0+4=4
		thread=Thread-22-b-1-executor[7 7], object=MySumCombinerAggregator@1842040246: zero
		thread=Thread-22-b-1-executor[7 7], object=MySumCombinerAggregator@1842040246: combine, 0+0=0
		thread=Thread-22-b-1-executor[7 7], object=MySumCombinerAggregator@1842040246: combine, 0+4=4
		thread=Thread-18-b-2-executor[8 8], object=PrinterFunction@929426307: value=4
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2037800273: num0=1, isKeep=false
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@654519276: num0=2, isKeep=true
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@654519276: num0=6, isKeep=true
		*/
	}
	
	
	/**
	 * 测试聚合函数链
	 * 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void test04() throws Exception {
		TridentTopology topology = new TridentTopology();
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3, 
				new Values(1,3), 
				new Values(2,3), 
				new Values(6,2), 
				new Values(4,2));
		spout.setCycle(true);
		
		topology.newStream("tx-1", spout)
				.shuffle().each(new Fields("a", "b"), new CheckEvenFirstFilter()).parallelismHint(3)
				.shuffle().each(new Fields("a","b"), new SumFunction(), new Fields("sum")).parallelismHint(2)
				.chainedAgg() 	//聚合链开始
				.aggregate(new Fields("a"), new MySumAggregator(), new Fields("aggSum"))
				.aggregate(new Fields("a"), new MyAvgAggregator(), new Fields("aggAvg"))
				.chainEnd() 	//聚合链结束
				.global().each(new Fields("aggSum", "aggAvg"), new PrinterFunction(), new Fields());
				
		boolean localMode = true;
		if (localMode) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("myapp", config, topology.build());
		} else {
			StormSubmitter.submitTopology("myapp", config, topology.build());
		}
		Thread.sleep(10000);
		
		/*
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@42801060: prepare
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@68294790: prepare
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1123622293: prepare
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2137168009: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1295729161: prepare
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@860137761: prepare
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@68294790: num0=6, isKeep=true
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2137168009: num0=1, isKeep=false
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@2137168009: num0=2, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@42801060: num0=2, num1=3, sum=5
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1123622293: num0=6, num1=2, sum=8
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@102799423: init, batchIdClass=TransactionAttempt, batchId=1:0
		thread=Thread-30-b-2-executor[9 9], object=MyAvgAggregator@2080299373: init, batchIdClass=TransactionAttempt, batchId=1:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@102799423: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@860137761: num0=8, num1=4.0, sumResult=12.0
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@68294790: num0=4, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@42801060: num0=4, num1=2, sum=6
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@102799423: init, batchIdClass=TransactionAttempt, batchId=2:0
		thread=Thread-30-b-2-executor[9 9], object=MyAvgAggregator@2080299373: init, batchIdClass=TransactionAttempt, batchId=2:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@102799423: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@860137761: num0=4, num1=4.0, sumResult=8.0
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@68294790: num0=1, isKeep=false
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@68294790: num0=2, isKeep=true
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@68294790: num0=6, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@42801060: num0=2, num1=3, sum=5
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@1123622293: num0=6, num1=2, sum=8
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@102799423: init, batchIdClass=TransactionAttempt, batchId=3:0
		thread=Thread-30-b-2-executor[9 9], object=MyAvgAggregator@2080299373: init, batchIdClass=TransactionAttempt, batchId=3:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@102799423: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@860137761: num0=8, num1=4.0, sumResult=12.0
		*/
	}
	
	
	/**
	 * 使用FixedBatchSpout测试persistence聚合操作
	 * 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void test05() throws Exception {
		TridentTopology topology = new TridentTopology();
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3, 
				new Values(1,3), 
				new Values(2,3), 
				new Values(6,2), 
				new Values(4,2));
		spout.setCycle(true);
		
		topology.newStream("tx-1", spout)
				.shuffle().each(new Fields("a", "b"), new CheckEvenFirstFilter()).parallelismHint(3)
				.shuffle().each(new Fields("a","b"), new SumFunction(), new Fields("sum")).parallelismHint(2)
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("a"), new MySumReducerAggregator(), new Fields("sum"))
				.newValuesStream()
				.global().each(new Fields("sum"), new PrinterFunction(), new Fields());
		
		boolean localMode = true;
		if (localMode) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("myapp", config, topology.build());
		} else {
			StormSubmitter.submitTopology("myapp", config, topology.build());
		}
		Thread.sleep(10000);
		
		/*
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: prepare
		thread=Thread-36-b-1-executor[5 5], object=CheckEvenFirstFilter@1712163831: prepare
		thread=Thread-28-b-1-executor[6 6], object=CheckEvenFirstFilter@1066388298: prepare
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: prepare
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@52655136: prepare
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@1688123317: prepare
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=1, isKeep=false
		thread=Thread-28-b-1-executor[6 6], object=CheckEvenFirstFilter@1066388298: num0=6, isKeep=true
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=2, isKeep=true
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@1688123317: num0=6, num1=2, sum=8
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@1688123317: num0=2, num1=3, sum=5
		thread=Thread-40-b-0-executor[4 4], object=MySumReducerAggregator@815259901: init
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: value=8
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=4, isKeep=true
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@52655136: num0=4, num1=2, sum=6
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: value=12
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=1, isKeep=false
		thread=Thread-28-b-1-executor[6 6], object=CheckEvenFirstFilter@1066388298: num0=2, isKeep=true
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=6, isKeep=true
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@1688123317: num0=2, num1=3, sum=5
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@52655136: num0=6, num1=2, sum=8
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: value=20
		thread=Thread-28-b-1-executor[6 6], object=CheckEvenFirstFilter@1066388298: num0=4, isKeep=true
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@1688123317: num0=4, num1=2, sum=6
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: value=24
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=2, isKeep=true
		thread=Thread-28-b-1-executor[6 6], object=CheckEvenFirstFilter@1066388298: num0=6, isKeep=true
		thread=Thread-36-b-1-executor[5 5], object=CheckEvenFirstFilter@1712163831: num0=1, isKeep=false
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@52655136: num0=6, num1=2, sum=8
		thread=Thread-18-b-2-executor[8 8], object=SumFunction@52655136: num0=2, num1=3, sum=5
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: value=32
		thread=Thread-22-b-1-executor[7 7], object=CheckEvenFirstFilter@1488605798: num0=4, isKeep=true
		thread=Thread-30-b-2-executor[9 9], object=SumFunction@1688123317: num0=4, num1=2, sum=6
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@1386651941: value=36
		*/
	}
	
	
	/**
	 * 测试groupBy操作
	 * 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void test06() throws Exception {
		TridentTopology topology = new TridentTopology();
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3, 
				new Values(1,3), 
				new Values(2,3), 
				new Values(6,2), 
				new Values(4,2));
		spout.setCycle(true);
		
		topology.newStream("tx-1", spout)
				.shuffle().each(new Fields("a", "b"), new CheckEvenFirstFilter()).parallelismHint(3)
				.shuffle().each(new Fields("a","b"), new SumFunction(), new Fields("sum")).parallelismHint(2)
				.groupBy(new Fields("a"))
				.aggregate(new Fields("a"), new MySumAggregator(), new Fields("aggSum"))
				.global().each(new Fields("aggSum"), new PrinterFunction(), new Fields());
		
		boolean localMode = true;
		if (localMode) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("myapp", config, topology.build());
		} else {
			StormSubmitter.submitTopology("myapp", config, topology.build());
		}
		Thread.sleep(10000);
		
		/*
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: prepare
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@99706709: prepare
		thread=Thread-28-b-0-executor[6 6], object=CheckEvenFirstFilter@1779408618: prepare
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@961844530: prepare
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@52655136: prepare
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@973872138: prepare
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@973872138: num0=1, isKeep=false
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@961844530: num0=6, isKeep=true
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@973872138: num0=2, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@99706709: num0=2, num1=3, sum=5
		thread=Thread-22-b-1-executor[7 7], object=SumFunction@52655136: num0=6, num1=2, sum=8
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: init, batchIdClass=TransactionAttempt, batchId=1:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: init, batchIdClass=TransactionAttempt, batchId=1:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: complete
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: value=2
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: value=6
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@973872138: num0=4, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@99706709: num0=4, num1=2, sum=6
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: init, batchIdClass=TransactionAttempt, batchId=2:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: value=4
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@973872138: num0=6, isKeep=true
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@961844530: num0=1, isKeep=false
		thread=Thread-40-b-0-executor[4 4], object=CheckEvenFirstFilter@961844530: num0=2, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@99706709: num0=6, num1=2, sum=8
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@99706709: num0=2, num1=3, sum=5
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: init, batchIdClass=TransactionAttempt, batchId=3:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: init, batchIdClass=TransactionAttempt, batchId=3:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: complete
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: value=2
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: value=6
		thread=Thread-36-b-0-executor[5 5], object=CheckEvenFirstFilter@973872138: num0=4, isKeep=true
		thread=Thread-18-b-1-executor[8 8], object=SumFunction@99706709: num0=4, num1=2, sum=6
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: init, batchIdClass=TransactionAttempt, batchId=4:0
		thread=Thread-30-b-2-executor[9 9], object=MySumAggregator@63601325: complete
		thread=Thread-38-b-3-executor[10 10], object=PrinterFunction@565615471: value=4
		*/
	}
	
	
	
	
}
