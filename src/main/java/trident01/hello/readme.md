
### Trident的五类操作：
	
	There are five types of operations that can be performed on streams in Trident
	1.Partiton-Local Operations: Operations that are applied locally to each partition and do not involve network transfer
	2.Repartitioning Operations: Operations that change how tuples are partitioned across tasks(thus causing network transfer), 
	  but do not change the content of the stream.
	3.Aggregation Operations: Operations that *may* repartition a stream (thus causing network transfer)
	4.Grouping Operations: Operations that may repartition a stream on specific fields and group together tuples whose fields 
	  values are equal.
	5.Merge and Join Operations: Operations that combine different streams together.
	
	a) 作用在本地分区上的操作，不会产生网络的传输
		function(inputFields, function, outputFields)
		filter(inputFields, filter)
		partitionAggregate(inputFields, aggregator|combinerAggregator|reducerAggregator, functionFields)
		stateQuery(state, [inputFields,] qu eryFunction, functionFields)
		partitionPersist(stateFactory, inputFields, stateUpdater)
		project(keepFields)
		
	b) 对流进行重分区，不改变流的内容而是会产生新的网络传输
		shuffle
		partitionBy
		global
		broadcast
		batchGlobal
		partition
	
	c) 聚合操作，有可能会产生网络传输
		aggregate()
		persistentAggregate()		
	
	d) 作用在分组流上的操作
		groupBy
	
	e) 合并和连接操作
		merge(stream1, stream2, stream3)
		join(stream1, fields1, stream2, fields2, fields3)
		
	
### 常见操作示例
	
	// 函数
	public class SumFunction extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer num0 = tuple.getInteger(0);
			Integer num1 = tuple.getInteger(1);
			Integer sum = num0 + num1;
			collector.emit(new Values(sum));
		}
	}
	
	// 过滤
	public class CheckEventSumFilter extends BaseFilter {
		@Override
		public boolean isKeep(TridentTuple tuple) {
			Integer num0 = tuple.getInteger(0);
			Integer num1 = tuple.getInteger(1);
			Integer sum = num0 + num1;
			return sum%2==0 ? true : false;
		}
	}
	
	// 投影
	x,y,z----->mystream.project(new Fields("x","y"))----->x,y
	
	
	// 随机分区 shuffle repartitioning
	mystream.shuffle().each(new Fields("a","b"), new MyFilter()).parallelismHint(2)
	partition0:
	1,3,5            partition0
	2,4,6            1,3,5
	7,9,10           2,4,6
	myspout------------------------>myfilter
	       |shuffle
	       |
	       |         partition1
	       |         7,9,10
	       +----------------------->myfilter
	 
	 
	// 按照字段分区 partitionBy repartitioning
	mystream.shuffle().each(new Fields("username")).each(new Fields("username", "text"), new MyFilter).parallelismHint(2)
	注意：partition=hash(fields)%(number of target partition)
	partition0:
	jhon,4            partition0
	lita,7            jhon,4
	jhon,8            jhon,8
	myspout----------------------------->myfilter
	       |partitionBy("username")
	       |
	       |         partition1
	       |         lita,7
	       +----------------------------->myfilter
	
	
	// 全局分区 global repartitioning
	mystream.global().each(new Fields("a","b"), new MyFilter()).parallelismHint(2)
	partition0:      partition0:
	1,3,5            1,3,5 
	2,4,6            2,4,6
	7,9,10           7,9,10 
	myspout------------------------>myfilter
	       |global
	       |
	       +----------------------->myfilter
	
	
	// 批次级别的全局分区 batchGlobal repartitioning
	// 对于一个批次的消息进到同一个分区中，但是从全局来看，消息的路由还是随机的
	BATCH1:
	partition0:      partition0:
	1,3,5            1,3,5 
	2,4,6            2,4,6
	7,9,10           7,9,10 
	myspout------------------------>myfilter
	       |batchGlobal
	       |
	       +----------------------->myfilter
	
	BATCH2:
	partition0: 
	3,2,1          
	4,3,6
	6,5,4 
	myspout------------------------>myfilter
	       |batchGlobal
	       |
	       |      partition0: 
	       |      3,2,1 
	       |      4,3,6
	       |      6,5,4 
	       +----------------------->myfilter
	
	
	// 广播分区 broadcast repartitioning
	mystream.broadcast().each(new Fields("a","b"), new MyFilter()).parallelismHint(2)
	partition0:      partition0:
	1,3,5            1,3,5 
	2,4,6            2,4,6
	7,9,10           7,9,10 
	myspout------------------------>myfilter
	       |broadcast
	       |
	       |         partition1
	       |         1,3,5 
	       |         2,4,6 
	       |         7,9,10
	       +----------------------->myfilter 
	
	
	// 自定义分区 custom repartitioning
	XxxRepartition implements CustomStreamGrouping {...}
	

### 聚合操作示例
	
	聚合操作可以基于三种方式，即 基于partition、基于batch、基于stream。
	注意，聚合操作之后将会覆盖之前的tuple，从而重新发送聚合之后的tuple数据到下一个处理节点。
	
	[基于partition的聚合操作]
	拓扑中每处理一个批次的tuple数据，都会在相应每个分区中进行一次聚合统计操作。
	mystream.partitionAggregate(new Fields("a"), new Count(), new Fields("count"))
	
	[基于batch的聚合操作]
	拓扑中每处理一个批次的tuple数据，先将这一批次所有分区的tuple进程global再分区将其汇集到一个分区中，再进行聚合运算。
	mystream.aggregate(new Fields("a"), new Count(), new Fields("count"))
	
	[基于stream的聚合操作]
	拓扑中每处理一个批次的tuple数据，都会在全局进行一次聚合运算，聚合的结果将累计到之前的state[可以是内存或数据库等]中。
	mystream.persistentAggregate(new MemoryMapState.Factory(), new Fields("a"), new MySumReducerAggregator(), new Fields("sum"))
	
		小插曲1：三种聚合函数接口（上述的 new Count()就是其中的CombinerAggregator），可实现自定义的聚合操作
		----------------------------------------------------------------------------------------
		ReducerAggregator	一般在partition聚合或stream聚合中使用
		Aggregator		一般在batch聚合中使用
		CombinerAggregator	先在每个partition运行分区聚合，然后global再分区将同一批次的分区聚合结果分到一个partition中，
		                    最后在这一个partition中聚合得到最终结果，这种方式的流量占用明显小于前两者。（可以对比MR的执行）
		
		public interface ReducerAggregator<T> extends Serializable {
		    T init();
		    T reduce(T curr, TridentTuple tuple);
		}
		public interface Aggregator<T> extends Operation {
		    // 在开始聚合之前调用，主要用于保存状态，供下面的两个方法使用
		    T init(Object batchId, TridentCollector collector);
		    // 迭代batch的每个tuple，处理每个tuple之后更新state状态
		    void aggregate(T val, TridentTuple tuple, TridentCollector collector);
		    // 所有的tuple处理完成之后调用，对于每一个batch返回单个tuple
		    void complete(T val, TridentCollector collector);
		}
		public interface CombinerAggregator<T> extends Serializable {
		    // 在每个tuple到来时运行的初始化方法
		    T init(TridentTuple tuple);
		    // 合成tuple的值，并输出含有一个值的tuple
		    T combine(T val1, T val2);
		    // 如果分区不含有tuple，则将调用该方法
		    T zero();
		}
		
		trident内置的一些聚合函数接口实现都在org.apache.storm.trident.operation.builtin下面，包括：
		ComparisonAggregator
		Count
		Debug
		Equals
		FilterNull
		FirstN
		MapGet
		Max
		MaxWithComparator
		Min
		MinWithComparator
		Negate
		SnapshotGet
		Sum
		TupleCollectionGet
		
		小插曲2：聚合函数链 Aggregator Chaining
		----------------------------------------------------------------------------------------
		对于同一个stream应用多个聚合函数，通过链化聚合函数进行分支聚合，最后合成一个元组（含有多个聚合结果）
		mystream.chainedAgg().partitionAggregate(new Fields("b"), new Average(), new Fields("avgerage"))
		                     .partitionAggregate(new Fields("b"), new Sum(), new Fields("sum"))
		                     .chainEnd()
		                     .global().each(new Fields("average", "sum"), new PrinterFunction(), new Fiels());
		
	     mystream----->AggregatorChaining-------->SumAggregator------------>result single tuple
	                                     |                               |
	                                     +------->AverageAggregator------+
	
	                                     
		小插曲3：聚合前使用groupby操作
		----------------------------------------------------------------------------------------
		groupby操作并不会涉及到任何再分区，而是会将输入流inputStream转换成分组流，从而修改后续的聚合函数的行为。
		如果groupby操作被用在partition分区之前，那么每个分区的聚合操作将会运行在分区的每个分组上；
		如果groupby操作被用在batch分区之前，那么同一批次的tuple将首先被重分区到同一个partition，然后在这个分区的每个分组上执行聚合操作。
		
          partition0:                         partition0:
          [hi]                                  group0:
          [test]                                 [hi]
          [hello]         groupby &              [hi]
          [hi]         partition aggregate      group1:
                      --------------------->     [test]  
                                                group2:
                                                 [hello]
          
          partition1:                          prtition1:
          [test]                                 group0:
          [hello]                                 [test]
          [test]                                  [test]
                                                 group1:
                                                  [hello]
		
	
### 非事务性topology和事务性topology
	
	非事务性拓扑：
		storm输出batch时，不保证在哪个bantch中。主要分为两种管道类型：
		a) 最多处理一次（at most）
		   失败的tuple不会进行重新发送，storm不执行ack操作
		b) 最少处理一次（at least）
		   失败的tuple会再次进入处理的管道中，至少处理一次
		   class Xxx implements IBatchSpout {
		       ...
		       public void ack(Object batchId) {
		           ...
		       }
		   }
	
	事务性拓扑：
		引入数据库，存储txid，失败时判断txid的值，是否累加还是还是重复处理。
		a) 每个batch拥有一个唯一的txid，失败时整个batch会重放
		b) batch中的tuple不会和其他batch中的tuple混合。
	
	
	
### DRPC分布式远程调用
	
	DRPC用于从拓扑中查询或取出结果。storm内置了drpc服务，DRPC Server接收客户端请求并将其传递到topology，topology处理请求并将结果发送给DRPC Server，
	最后DRPC Server将结果重定向给客户端。DRPC Server可以在storm.yml文件中通过如下参数配置：
	
	drpc.sevrers:
	    - "server01"
	    - "server02"
	
	启动drpc服务器:
	$ storm drpc
	

### storm整合hbase
	
	步骤：
	a) 创建hbase的表
	b) 创建storm的spout，随机生成测试数据
	c) 创建StormHbaseBolt.java
	d) 
	

	
	
	
	
	
	
	     
	       