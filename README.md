
## Storm Introduce

### nimbus、supervisor

	storm 常见守护进程：
		nimbus
		ui
		drpc
		logviwer
		supervisor
		
### worker（进程）
	
	storm 集群的一个节点上可能有一个或多个拓扑上，一个工作进程执行拓扑的一个子集。工作进程属于一个特定的拓扑，
	并可能为这个拓扑的一个或多个组件（spout、bolt）运行一个或多个执行器。一个运行中的拓扑包括多个运行在 storm 
	集群内多个节点的进程。拓扑的工作进程数可以通过 Config.TOPOLOGY_WORKERS 设置。

![storm-worker-process](../master/imgs/storm-worker-process.png)

		
### executor（线程，运行一个拓扑的一种组件任务[spout或bolt]）
	
	一个或多个执行器可能运行在一个或多个工作进程内，执行器是由工作进程产生的一个线程，它为相同的组件（spout、bolt）
	运行一个或多个任务（task）。每个组件的执行器的初始数量可以通过 TopologyBuilder#setSpout 或 TopologyBuilder#setBolt 
	的 parallelism_hint 参数设置。可以通过 Config.TOPOLOGY_MAX_TASK_PARALLELISM 或 config.setMaxTaskParallelism(3) 
	来配置该选项。
	
### task（逻辑概念，运行在执行器中的任务实例，它是最终完成数据处理的实体单元）
	
	一个组件的任务数量始终贯穿拓扑的整个生命周期，但一个组件的执行器（线程）数量可以随时间而改变，这意味着 #threads <= #tasks。
	默认情况下任务的数量被设定为相同的执行器数量，即 storm 会用一个线程 executor 执行一个 task 任务。每个组件的任务数量可以通过 
	Config.TOPOLOGY_TASKS 设置，某个组件的任务数量可以通过如下方式设置：
	topologyBuilder.setBolt("xxxBolt", new XxxBolt(), 3)
				.setNumTasks(6) //三个执行器，每个执行器线程运行两个任务
				.shuffleGrouping("xxxSpout")
				
### rebalancing
	
	storm 的一个很好的特性是，可以增加或者减少工作进程数（worker）或 执行器（executor）的数量而不需要重新启动集群或拓扑，
	这个过程被称为 storm的再平衡。
	有两种方式实现拓扑再平衡，使用 storm ui 或者 使用 cli shell:
	$ storm rebanlance xxxTopology -n 5 -e xxxSpout=3 -e xxxBolt=10
	
### topology

	运行拓扑：
	要使用 storm 做实时计算，首先要创建所谓的拓扑，一个拓扑实际上是一个有向无环图的计算，拓扑中的每个节点包含处理逻辑，节点间的
	连接表示数据如何在节点间传递。TopologyBuilder 是构建拓扑的类。创建和提交拓扑的过程，首先，new TopologyBuilder，然后调
	用 setSpout 和 setBolt 设置 spout 和 bolt 组件，最后调用 createTopology 方法返回 StormTopology 对象给 	
	StormSubmitter#submitTopology 或 LocalCluster#submitTopology（本地）方法作为输入参数。
	因为拓扑的定义是 Thrift 结构，而 nimbus 是一个 thrift 服务，所以可以使用任何编程语言来创建和提交 topology。
	
	拓扑的运行是很简单的。首先打包所有代码到一个单独的 jar 包中，然后运行如下命令：
	$ storm xxx.jar xxx.xxxTopology args...
	
	杀死一个拓扑：
	$ storm kill topoName
	storm 不会立即杀死拓扑，而是先使所有的 spout 失效，这样它们就不会发送新的 tuple
	storm 等待若干秒后（该时间由 Config.TOPOLOGY_MESSAGE_TIMOUT_SES 指定）
	摧毁所有的 worker，这就给拓扑足够的时间来完成已经存在的 tuple 的处理工作。
	
	监控拓扑：
	运行 storm ui 进程，它提供了有错误发生的任务、吞吐量的细粒度统计、每个运行中拓扑中每个组件的延迟性能等信息。
	$ nohup storm ui &
	
### tuple
	
	分为 普通的tuple 和 tick tuple：
	Storm 0.8.0以后中内置了一种定时机制——tick，它能够让任何bolt的所有task每隔一段时间（精确到秒级，用户可以自定义）收到一个
	来自__systemd的__tick stream的tick tuple，bolt收到这样的tuple后可以根据业务需求完成相应的处理。若希望某个bolt每隔一
	段时间做一些操作，那么可以将bolt继承BaseBasicBolt/BaseRichBolt，并重写getComponentConfiguration()方法。在方法中设置
	Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS的值，单位是秒。这样设置之后，此bolt的所有task都会每隔一段时间收到一个来自
	__systemd的__tick stream的tick tuple。若希望Topology中的每个bolt都每隔一段时间做一些操作，那么可以定义一个Topology全
	局的tick，同样是设置Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS的值。
	Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS是精确到秒级的。例如某bolt设置Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS为10s，
	理论上说bolt的每个task应该每个10s收到一个tick tuple。实际测试发现，这个时间间隔的精确性是很高的，一般延迟（而不是提前）
	时间在1ms左右。测试环境：3台虚拟机做supervisor，每台配置：4Cpu、16G内存、千兆网卡。

### 序列化
	
	一般配置：
	storm 使用 kryo 实现序列化和反序列化，默认情况下可以对 原始类型、字符串、字符数组、ArrayList、HashMap、HashSet 和
	Clojure集合进行序列化，如果想要在元组中使用另一种类型，则需要注册一个自定义的 序列化器。添加自定义序列化器是通过配置的 
	topology.kryo.register 属性完成的，它需要一个注册的列表，每个注册项可以采取以下两种形式：
	a) 类名注册，在这种情况下，storm 将使用 kryo 的 FieldsSerializer 来序列化该类
	b) 继承了 com.esotericsoftware.kryo.Serializer 类的类名注册
	
	topology.kryo.register
	- test.kryo.XXX
	- test.kryo.User: test.kryo.User.UserSerializer
	
	高级配置：
	Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS  true表示 storm 会忽略任何已经注册但在类路径中没有其代码的序列化，
	负责false在这种情况下抛出异常
	Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION  true表示 storm 遇到一种没有序列化注册的类型，可能的话会使用java
	原生的序列化（比较消耗资源）
	
### stream
	
	略
		
### stream grouping

	storm 内置了 8 种流分组的方式，通过实现 CustomStreamGrouping 接口可以实现自定义的流分组。
	InputDeclarer 接口定义了不同的流分组方式，每当 TopologyBuilder#setBolt 方法被调用就返回该对象，用于声明一个bolt的输入
	流以及这些流应当如何分组。该接口定义的所有分组方法如下：
	
	1、随机分组（shuffleGrouping）
	   最常用的分组方式，它随机地分发元组到 bolt 上的任务，这样能保证每个任务得到基本相同数量的元组。
	   例如如果希望 bolt2 读取 spout 和 bolt1 两个组件发送的tuple，则可以定义 bolt2 如下：
	   topologyBuilder.setBolt("bolt2", new Bolt2(), 5)
	   			.shuffleGrouping("spout")
	   			.shuffleGrouping("bolt1");
	
	2、无分组（noneGrouping）
	   假定你不关心流是如何被分组的，则可以使用这种方式，目前这种分组和随机分组是一样的效果，有一点不同的是 storm 会把这个bolt 
	   放到其订阅者的同一个线程中执行。
	
	3、本地或随机分组（localOrShuffleGrouping）
	   如果目标 Bolt 中的一个或者多个 Task 和当前产生数据的 Task 在同一个Worker 进程里面，那么就走内部的线程间通信，将Tuple
	   直接发给在当前 Worker 进程的目的 Task。否则，同 shuffleGrouping。localOrShuffleGrouping 的数据传输性能优于 
	   shuffleGrouping，因为在 Worker 内部传输，只需要通过Disruptor队列就可以完成，没有网络开销和序列化开销。因此在数据处理
	   的复杂度不高， 而网络开销和序列化开销占主要地位的情况下，可以优先使用 localOrShuffleGrouping来代替 shuffleGrouping。
	
	4、字段分组（fieldsGrouping）
	   根据指定字段对流进行分组。例如，如果是按 userid 字段进行分组，具有相同 userid 的元组被分发到相同的任务，具有不同 userid 
	   的元组可能被分发到不同的任务。字段分组是实现流连接和关联、以及大量其他用例的基础，在实现上，字段分组使用取模散列来实现。
	
	5、部分关键字分组（partialKeyGrouping）
	   这种方式与字段分组很相似，根据定义的字段来对数据流进行分组，不同的是，这种方式会考虑下游 Bolt 数据处理的均衡性问题，在输入数
	   据源关键字不平衡时会有更好的性能。
	
	6、广播分组（allGrouping）
	   流被发送到所有 bolt 的任务中，使用这个分组方式要特别小心。
	
	7、全局分组（globalGrouping）
	   全部流被发送到 bolt 的同一个任务中（id 最小的任务）。
	
	8、直接分组（directGrouping）
	   由元组的生产者决定元组消费者的接收元组的任务，直接分组只能在已经声明为直接流（direct stream）的流中使用，声明方法为在 
	   declareOutFields方法中使用OutputFieldsDeclarer#declareStream 方法，并且元组必须使用 emitDirect 方法来发射。
	   Bolt 通过 TopologyContext 对象或者 OutputCollector 类的 emit 方法的返回值，可以得到其消费者的任务 id 列表
	  （List<Integer>）。
	
	9、自定义分组（customGrouping）
	   可以通过实现 CustomStreamGrouping 接口来创建自定义的流分组。
	   使用时通过 topologyBuilder.setBolt("bolt2", new Bolt2(), 5).customGrouping("a", new XxxGroupring());
	
### spout
	
	类图：
	IComponent
	  |--IRichStateSpout
	  |--IPartitionedTransactionalSpout
	    	  |--BasePartitionedTransactionalSpout
	    	  |--MemoryTransactionalSpout
	  |--IOpaquePartitionedTransactionalSpout*
	  |--ITransactionalSpout
	       |--ICommitterTransactionalSpout
	  |--ISpout
	       |--IRichSpout*
	           |--BaseRichSpout*
	           |--DRPCSpout
	      
	注意1：
	注意不要在 Spout 中处理耗时的操作。Spout 中 nextTuple 方法会发射数据流，在启用 Ack 的情况下，fail 方法和 ack 方法会被触发。
	需要明确一点，在 Storm 中，Spout 是单线程（JStorm的Spout分了3个线程，分别执行nextTuple方法、fail方法和ack 方法）。
	如果nextTuple方法非常耗时，某个消息被成功执行完毕后，Acker会给Spout发送消息，Spout若无法及时消费，可能造成ACK消息超时后被丢弃，
	然后Spout反而认为这个消息执行失败了，造成逻辑错误。反之若fail方法或者ack方法的操作耗时较多，则会影响Spout发射数据的量，造成 
	Topology 吞吐量降低。
	
	注意2：
	可以通过 Config.TOPOLOGY_MAX_SPOUT_PENDING 或 config.setMaxSpoutPending(5000) 设置spout缓存tuple的数量。其意义在于，
	当下流的 bolt 还有 topology.max.spout.pending 个 tuple 没有消费完时，spout会停下来，等待下游bolt去消费，当tupl 的个数少于 
	topology.max.spout.pending 个数时，spout 会继续从消息源读取消息（这个属性只对可靠消息处理有用）。默认情况下，这个参数的值为 
	null（表示值为1）。在jstorm中，topology.max.spout.pending 的意义与 storm 有所不同，设置不为1时（包括设置为null），spout 
	内部将额外启动一个线程单独执行 ack 或 fail 操作， 从而 nextTuple 在单独一个线程中执行，因此允许在nextTuple中执行block动作，
	而原生的storm，nextTuple/ack/fail都在一个线程中执行，当数据量不大时，nextTuple 立即返回，而 ack、fail 同样也容易没有数据，
	进而导致 CPU 大量空转，白白浪费 CPU，而在 JStorm 中，nextTuple
	可以 block 方式获取数据，比如从 disruptor 中或 BlockingQueue 中获取数据，当没有数据时，直接 block 住，节省了大量 CPU。
	
	注意3：
	一些方便的 spout 包组件：
	kafka 作为数据源，storm-kafka
	Kestrel 作为数据源，storm-kestrel
	AMQP 作为数据源，storm-amqp-spout
	JMS 作为数据源，storm-jms
	redis 作为数据源，storm-redis-pubsub
	
### bolt
	ICommitter
	IComponent
	  |--IBatchBolt
	       |--BaseBatchBolt
	           |--BaseTransactionalBolt*
	  |--IRichBolt
	       |--BaseRichBolt*
	       |--CoordinatedBolt*
	  |--IBasicBolt
	       |--BaseBasicBolt*
	
	使用普通的实现 IRichBolt 的接口的 bolt 实现消息的锚定和确认：
	public class MyBolt implements IRichBolt {
		// ...
		@Override
		public void execute(Tuple tuple) {
			String message = tuple.getString(0);
			collector.emit(tuple, new Values(message)); //锚定和发射
			collector.ack(tuple); //确认
		}
		// ...
	}
	
	使用 IBasicBolt 接口可以实现Tuple的自动锚定和确认，它会在调用 bolt#execute 方法之后正确调用 
	OutputCollector#ack 方法来时来实现自动确认。例如下面的代码 SplitSentence类的execute方法中，
	元组被发射到 BasicOutputCollector 后自动锚定到输入元组，execute 执行完后自动确认消息。
	public class SplitSentence extends BaseBasicBolt {
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String sentence = tuple.getString(0);
			for (String word : sentence.split(" ")) {
				collector.emit(new Values(word));
			}
		}
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
	
	Spout消息到Bolt的执行过程：
	假设同属于一个Topology的Spout与Bolt分别处于不同的JVM，即不同的worker中，不同的JVM可能处于同一台物理机器，也可能处于不同的物理机器中。
	为了让情景简单，认为JVM处于不同的物理机器中。
	a) spout的输出通过该spout所处worker的消息输出线程，将tuple输入到Bolt所属的worker。它们之间的通路是socket连接，用ZeroMQ或Netty实现。
	b) bolt所处的worker有一个专门处理socket消息的receive thread 接收到spout发送来的tuple。
	c) receive thread将接收到的消息传送给对应的bolt所在的executor。 在worker进程内部,消息传递使用的是Lmax Disruptor pattern。
	d) executor接收到tuple之后，由event-handler进行处理。
	
	CoordinatedBolt的原理:
	对于用户在DRPC, Transactional Topology里面的Bolt，都被CoordinatedBolt包装了一层：也就是说在DRPC, Transactional Topology
	里面的topology里面运行的已经不是用户提供的原始的Bolt, 而是一堆CoordinatedBolt, CoordinatedBolt把这些Bolt的事务都代理了。
	
### hook

	storm 提供了钩子，使用它可以在 storm 内部插入自定义代码来运行任意数量的事件。可以通过实现 ITaskHook 或继承 BaseTaskHook 
	类创建一个 hook，为要捕获的事件
	重写适当的方法。有两种方法来注册自定义 hook：
	a) 在 Spout#open 或 Bolt#prepare 方法中，使用 TopologyContext#addTaskHook
	b) 在 storm 配置中使用 Config.TOPOLOGY_AUTO_TASK_HOOKS 配置，这些钩子在每个 spout 或 bolt 中自动注册，
	   它们对于在自定义的监控系统中进行集成是很有用的。

### anchering emit & ack or fail process
	
	发射消息：
	spout 和 bolt 分别使用 SpoutOutputCollector 和 OutputCollector 发射消息（emit），并且 SpoutOutputCollector 和
	OutputCollector 是线程安全的，可以作为组件的成员变量进行保存。anchering 和发射一个新的元组在同一时间完成，一个输出元组可
	以被锚定到多个输入元组，称为复合锚定，一个复合锚定元组未能被处理将导致来自 spout 的多个元组重发。spout 发射消息到 bolt，
	同时 storm 负责跟踪创建的消息树，如果 storm 检测到一个元组是完全处理的，则 storm 将调用原 spout的 ack方法，把spout提供
	给storm的消息 id 作为输入参数传入，进行消息的成功处理。反之，调用 spout#fail。

	消息被完全处理的含义：
	如同“蝴蝶效应”一样，一个来自 spout 的元组可以引发基于它所创建的数以千计的元组。消息被完全处理的含义是tuple树创建完毕，并且树
	中的每一个消息都已被处理。当一个元组的消息树在指定的超时范围内不能被完全处理，则元组被认为是失败的。超时的时间默认是 30s，对于
	一个特定的拓扑，可以使用 Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS 来修改。
	
	Acker任务：
	一个 storm 拓扑有一组特殊的 acker 任务，对于每一个 spout 元组，跟踪元组的有向无环图。可以在拓扑配置中使用 
	Config.TOPOLOGY_ACKERS 为一个拓扑设置 acker的任务数量，storm 默认 TOPOLOGY_ACKERS 是1个，对于拓扑处
	理大量的信息，需要增加这个数字。
	
	删除可靠性保证：
	有三种方法可以删除可靠性保证，如下：
	第一种是设置 Config.TOPOLOGY_ACKERS 为 0，在这种情况下，storm 会在 spout 发射一个元组之后立即调用 spout#ack 方法，元组树不会被跟踪；
	第二种是通过消息基础删除消息的可靠性，可以在 SpoutOutputCollector#emit 方法中忽略x消息的 id，关掉对于个别 spout 元组的跟踪；
	第三种做法，如果你不关心拓扑的下游元组的特定子集是否无法处理，可以作为非固定元组（不锚定）发射它们，因为它们没有锚定到任何 spout 元组，
	所以如果它们没有 acked，不会造成任何 spout 元组失败。
	
	[注1]
	如果可靠性不是那么重要，那么不跟踪tuple树可以节省一半的消息，减少带宽占用。
	
	[注2]
	当一个tuple在拓扑中被创建出来的时候，不管是在Spout中还是在Bolt中创建的，这个 tuple都会被配置一个随机的64位id。
	acker就是使用这些id来跟踪每个spout tuple的 tuple DAG。这里贴一下storm源码分析里一个ack机制的例子。
         
                   T2
                   2
           |￣￣￣￣￣￣￣￣￣￣bolt2
           |                  |
           |                  | 5
           |                  |
           |        8         |/
         spout <------------>  acker bolt
           |        3          |\         |\
           |                   |            \
           |                   | 6           \ 7
           |                   |              \ 
           |                   |               \
           |__________________bolt1______________bolt3
                   1                    4
                   T1                T3 T4 T5 
     
     理解下整个大体节奏分为几部分:
     1、步骤1和步骤2中spout把一条信息同时发送给了bolt1和bolt2。
     2、​步骤3表示spout emit成功后去acker bolt里注册本次根消息，ack值设定为本次发送的消息对应的64位id的异或运算值，上图对应的是T1^T2。
     3、​步骤4表示bolt1收到T1后，单条tuple被拆成了三条消息T3、T4、T5发送给bolt3。
     4、步骤6表示bolt1在ack()方法调用时会向acker bolt提交T1^T3^T4^T5的ack值。
     5、步骤5和7的bolt都没有产生新消息，所以ack()的时候分别向acker bolt提交了T2 和T3^T4^T5的ack值。
     6、​综上所述，本次spout产生的tuple树对应的ack值经过的运算为 T1^T2^T1^T3^T4^T5^T2^T3^T4^T5按照异或运算的规则，ack值最终正好归零。
     7、​步骤8为acker bolt发现根spout最终对应的的ack是0以后认为所有衍生出来的数据都已经处理成功，它会通知对应的spout，spout会调用相应的ack方法。
     storm这个机制的实现方式保证了无论一个tuple树有多少个节点，一个根消息对应的追踪ack值所占用的空间大小是固定的，极大地节约了内存空间。
	
### 配置:
	
	优先级：
	defaults.yaml < storm.yaml < 特定拓扑的配置 < 内部特定组件的配置 < 外部特定组件的配置
	
	常见的配置：
	Config.TOPOLOGY_DEBUG
	Config.TOPOLOGY_WORKERS
	Config.TOPOLOGY_ACKS
	Config.TOPOLOGY_MAX_TASK_PARALLELISM
	Config.TOPOLOGY_TASKS
	Config.TOPOLOGY_MAX_SPOUT_PENDING
	Config.TOPOLOGY_MAX_MESSAGE_TIMEOUT_SECS
	Config.TOPOLOGY_SERIALIZATIONS
	
	注意：
	Config 类为所有可以配置的项提供了setter方法，它还提供了所有可配置的常量，可以在 default.yaml 文件找到找到默认值。
	可以把其他的配置项添加到 Config 类中，storm 会忽略其他不能识别的配置项，但拓扑可以在 Spout#open 或 Bolt#prepare 
	方法中自由地使用这些外在的配置项。
	
### 容错机制
	
	Worker进程死亡：
	当一个工作进程死亡，supervisor会尝试重启它，如果启动连续失败了一定的次数，无法发送心跳信息到 nimbus，则nimbus会在另一台
	主机上重新分配 Worker。
	
	Supervisor节点死亡：
	当一个Supervisor节点死亡，分配给该节点主机的任务会暂停，nimbus 会把这些任务重新分配给其他的节点主机。
	
	Nimbus 或 Supervisor 守护进程死亡：
	Nimbus 或 Supervisor 守护进程被设计成快速失败的（每当遇到任何意外的情况，进程自动毁灭）和 无状态的（所有状态信息都保存在zk或者磁盘上）。
	Nimbus 或 Supervisor 守护进程应该使用 daemontools 或 monit 工具监控运行。所以如果Nimbus 或 Supervisor 守护进程死亡，它们的重启
	就像什么事没有发生一样正常工作。Nimbus 或 Supervisor 守护进程死亡并不会影响 Worker 进程的工作。
	
	Nimbus 单点故障：
	如果失去了Nimbus节点，Worker也会继续执行，如果 Worker 死亡，supervisor也会重启他们。但是如果没有nimbus，Worker不会在必要
	时安排到其他主机。
	所以在“某种程度上”nimbus是单点故障，但在实践中这不是什么大问题，因为nimbus守护进程死亡，不会发生灾难性的问题，并且storm1.x版
	本以后，nimbus 实现了高可用（HA），可以通过 nimbus.seeds 设置多个 nimbus 节点。
	
	任务挂了导致元组没有被ack：
	在这种情况下，在树根的失败元组的 spout 元组 id 会超时并被重新发送。
	
	acker 任务挂了：
	在这种情况下，所有 spout 元组跟踪的 acker 会超时并被重新发送。
	
	
### drpc
	
	DRPC 示例：
	参考  test04.drpc 包
	

### 事务拓扑

	事务拓扑（Transactional Topology）是storm 0.7 引入的特性，在 0.8 版本中已经被封装为 Trident，提供了更加便利和直接的接口。
	引入事务拓扑的目的是为了满足对消息处理有着极其严格要求的场景（适合做汇总而不是过滤），例如实时计算某个用户的成交笔数，要求结果完
	全精确一致。事务拓扑可以实现一次只有一次的语义，它可以保证每个tuple”被且仅被处理一次”。storm 的事务拓扑是基于它底层的
	spout/bolt/acker 原语实现的。简单来说就是将元组分为一个个的 batch，同一个 batch 内的元组以及 batch 与 batch 之间的元组可
	以并行处理，另一方面，用户可以设置某些bolt 为 Commiter，storm 可以保证 Commiter 的 finishBatch() 操作按严格不降序的顺序
	执行。用户可以利用这个特性通过简单的编程技巧实现简单的消息处理的精确性。
	
	事务拓扑的核心是保证数据处理的严格有序：
	第一个设计：每次处理一个元组，在当前元组未被拓扑处理成功之前，不对下一个元组进行处理。（没有用到 storm 的并行处理能力，效率低）
	第二个设计：每个事物处理一批（batch）元组，并且 batch 之间的处理是严格有序的。（用到 storm 的并行处理能力，库操作也减少一些，但仍存在阻塞现象）
	第三个设计：storm的设计，batch 的处理过程中，并非所有的工作都需要严格有序。例如，全局计数的计算可以分成两个部分，即计算 batch 的部分计数、
	
	根据部分计数更新数据库中的全局计数。storm的 设计把 batch 的处理分成两个阶段：
	1) 处理阶段（processing phase），在处理阶段，可以并行处理 batch；
	2) 提交阶段（commit phase），在提交阶段，batch 之间是严格有序的。
	所以当 batch1 正在更新数据库时，其余的 batch 可以计算它们的部分计数。在提交阶段，在 batch1 的提交成功之前，batch2 的提交是不会执行的。
	处理阶段 和 提交阶段何在一起被称为一个 “事务”，包含事务的处理逻辑的拓扑称为 “事务拓扑”。在任意时刻，处理阶段可以有很多 batch，但提交阶段
	只能有一个 batch。如果一个 batch在处理阶段或者提交阶段有任何错误，则整个事务必须重新执行。
	
	如果使用事务拓扑，storm 会自动处理如下的事情：
	1) 管理状态：storm 在 zk 中保存执行事务拓扑的所有状态，状态主要包括当前事务 id、每个batch中的元数据等；
	2) 协调事务：storm 将管理所有需要决策的事情，例如在适当的时候处理或者提交适当的事务；
	3) 故障检测：当一个 batch 已经处理成功、提交或者更新失败时，storm 会利用 acking 框架有效地检测到。如果失败，storm 会在适当的时候重发 batch。
	   不必做任何 acking 或 anchoring 的工作，storm 会为你管理所有的一切；
	4) 封装批处理API：storm 在普通的 bolt 上封装了 API，支持元组的批处理，当任务已收到特定事务的所有元组，storm 协调所有的工作。storm 也会为每
	   个事务清理产生的中间数据。
	事务拓扑需要一个可以重发确切 batch 消息的消息队列系统，kestrel 无法做到这一点，apache kafka 非常合适。（原生kafka API 或 storm-kafka）
	
	
	事务性Spout和事务性Bolt
	--------—------------
	事务性的Spout需要实现ITopologySpout，这个接口包括两个内部接口类 Coodinator和Emitter，在topology运行的时候，
	事务性的Spout包含一个子Topology，结构如下：
	
	Coodinator task-----------Emitter task
	              |___________Emitter task
	              |___________Emitter task
	              
	a) 这里面有两种类型的tuple，一种是事务性的tuple，一种是batch中的tuple，coodinator开启一个事务准备发射一个batch时候，进入一个
	事务的processing阶段，会发射一个事务性tuple（transactionAttempt & matadata）到batch emit流。Emitter以all grouping
	的方式订阅coodinator的batch emit流，负责为每一个batch发射tuple，发射的tuple都必须以TransactionAttempt作为第一个field，
	storm根据这个field判断tuple属于哪一个batch。coodinator只有一个，emitter根据并行度可以有多个实例。
	b) TransactionAttempt包含两个值，一个transactionId，一个attemptId，transactionId对于每个batch中的tuple是唯一的，而且不管
	这个batch replay多少次都是一样的。attemptId是对于每一个batch唯一的一个id，但是对于同一个batch，它replay之后的attemptId跟
	replay之前就不一样了。我们可以把attemptId理解成replay-times，storm利用这个id区别一个batch发射tuple的不同版本。
	c) matadata（元数据）中包含当前事务可以从哪个point进行重放数据，存放在zookeeper中，spout可以通过Kyyo从zookeeper中序列化和
	反序列化该元数据。
	元组的发射请求是幂等的，需要 TransactionSpout保存少量的状态到 zk 中。
	IPartitionedTransactionalSpout：分区事务Spout从很多队列代理的分区中集中读取 batch，它自动管理每个分区的状态，保证等概率重发。
	
	事务性的Bolt（例如BaseTransactionalBolt）处理batch在一起的tuples，对于每一个tuple调用execute方法，而在整个batch处理阶段完成
	的时候调用finishBatch方法。如果BatchBolt被标记成Committer，则只能在commit阶段调用finishBatch方法。一个batch的commit阶段是由
	storm保证只在前一个batch成功提交之后才会执行。并且它会重试直到topology里面的所有bolt在commit完成提交。那么如何知道batch的process
	完成了？也就是bolt是否接收并处理了batch里的所有tuple？其实事务性bolt和drpc的bolt通常被CoordinatedBolt包装成了代理类，使用这个
	CoordinatedBolt模型即可完成上下游bolt之间的通信。每个CoordinatedBolt记录两个值，有哪些task给我发送了tuple（根据topo的groupring）
	，等所有的tuple都发送完成之后，CoordinatedBolt通过另外一个特殊的stream以emitDirect的方式告诉所有他发送过的tuple的task，它发送了
	多少个tuple给这个task，下游会将这个数字和自己已经收到的tuple数量做对比，如果相等则说明已经处理完了所有的tuple。下游的CoordinatedBolt
	会重复上面的步骤，通知其下游。
	
                             |￣事务bolt或drpc的bolt被CoordinatedBolt包装     |￣被CoordinatedBolt包装的Bolt
                             |  我处理完成上游tuples，是时候通知下游了！         |  我的上游通知我发送了N个tule，并且我处理完了上游的这N
                             |  我发N个tuple给你哈！                          |  个tuple，是时候通知下游了！
                             |                                              |
                             |                                              |
     Spout------->bolt1------>bolt2---------------------------------------->bolt3(commitor，标记需要事务处理)
                            （并行）                                        （串行，实现ICommiter或TxTopoBuilder.setCommiterBolt）
                             [batch3 processing...]                         [batch2, batch1 commiting(finishbatch)]
                             [botch4 processing... execute & finishbatch]	       	
	
	
	[注]标记为Committer的BatchBolt和普通BatchBolt的区别是调用 finishBatch 的时机。Committer BatchBolt 在提交阶段会调用
	   finishBatch，当所有的batch 都已经成功提交，提交阶段会出现，它会不停重试，直到【拓扑中的所有 bolt 成功提交 batch】。
	   有两种方法可以使 BatchBolt 成为 Committer BatchBolt，即实现ICommitter接口或者使用TransactionalTopologyBuilder
	   类的 setCommiterBolt 方法。
	
	
	IPartitionedTransactionalSpout和IOpaquePartitionedTransactionalSpout:
	--------—------------------------------------------------------------
	二者的相同之处：
	IPartitionedTransactionalSpout和IOpaquePartitionedTransactionalSpout都是把tuple封装成batch进行处理，同时可以保证每
	一个tuple都被完整地处理，都支持消息重发。为了支持事务性，它们为每一个批次（batch）提供一个唯一的事务ID（transaction id：txid）
	，txid是顺序递增的，而且保证对批次的处理是强有序的，即必须完整处理完txid=1才能再接着处理txid=2。
	
	二者的区别以及用法：
	IPartitionedTransactionalSpout的每一个tuple都会绑定在固定的批次中。无论一个tuple重发多少次，它都在同一个批次里面，都有同样
	的事务ID；一个tuple不会出现在两个以上的批次里。一个批次无论重发多少次，它也只有一个唯一且相同的事务ID，不会改变。这也就是说，一个
	批次无论重发多少次，它所包含的内容都是完全一致的。
	但是IPartitionedTransactionalSpout会有一个问题，虽然这种问题非常罕见：假设一批消息在被bolt消费过程中失败了，需要spout重发，
	此时如果正巧遇到消息发送中间件故障，例如某一个分区不可读，spout为了保证重发时每一批次包含的tuple一致，它只能等待消息中间件恢复，
	也就是卡在那里无法再继续发送给bolt消息了，直至消息中间件恢复。
	IOpaquePartitionedTransactionalSpout[I.O.P.T.Spout]为了解决这个问题，它不保证每次重发一个批次的消息所包含的tuple完全一
	致。也就是说某个tuple可能第一次在txid=2的批次中出现，后面有可能在txid=5的批次中出现。这种情况只出现在当某一批次消息消费失败需
	要重发且恰巧消息中间件故障时。这时，I.O.P.T.Spout 不是等待消息中间件故障恢复，而是先读取可读的partition。例如txid=2的批次在
	消费过程中失败了，需要重发，恰巧消息中间件的16个分区有1个分区(partition=3)因为故障不可读了。这时候I.O.P.T.Spout会先读另外的
	15个分区，完成txid=2这个批次的发送，这时候同样的批次其实包含的tuple已经少了。假设在txid=5时消息中间件的故障恢复了，那之前在
	txid=2且在分区partition=3的tuple会重新发送，包含在txid=5的批次中。
	在使用IOpaquePartitionedTransactionalSpout时，因为tuple与txid的对应关系有可能改变，因此与业务计算结果同时保存一个txid就无
	法保证事务性了。这时候解决方案会稍微复杂一些，除了保存业务计算结果以外，还要保存两个元素：前一批次的业务计算结果以及本批次的事务ID。
	我们以一个更简单的计算全局count的例子作说明，假设目前的统计结果为：
	{ value = 4,
	  prevValue = 1,
	  txid = 2
	}
	新的一批次txid=3的增量count是2
	可以保证完整事务性的计算应该是：检查新批次的txid与已保存的txid，如果两个txid相同，说明此批次消息已经来过。但是由于I.O.P.T.Spout
	不保证同批次消息重发后所包含的tuple和之前一致，因此这时候要重新计算这批次的值，即value = preValue + 新发来的增量。如果两个txid
	不相同，说明此批次消息不是重发的消息，那么value = value + 新发来的增量。根据这个计算逻辑，新计算后的结果应该为：
	{ value = 6,
	  prevValue = 4,
	  txid = 3
	}
	但是假设新的一批次的txid不是3而是2，增量count同样为2，那么新计算后的结果应该为：
	{ value = 3,
	  prevValue = 1,
	  txid = 2
	}
	I.O.P.T.Spout 接口同样有两个嵌套类：I.O.P.T.Spout.Coordinator 和 I.O.P.T.Spout.Emitter<X>
	相比IPartitionedTransactionalSpout.Emitter<X>的下面两个发射方法：
	a) X emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);
    	b) void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X partitionMeta);
	IOpaquePartitionedTransactionalSpout.Emitter<X>类的发射方法从2个变成了1个：
	a) X emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);
	它不区分发新消息还是重发旧消息，全部用emitPartitionBatch搞定。虽然emitPartitionBatch返回的X应该是下一批次供自己使用的
	（emitPartitionBatch的第4个参数），但是只有一个批次成功以后X才会更新到ZooKeeper中，如果失败重发，emitPartitionBatch读取的X还是旧的。
	所以这时候自定义的X不需要记录当前批次的开始偏移量和下一批次的开始偏移量两个值，只需要记录下一批次开始偏移量一个值即可，例如：
	public class BatchMeta { 
	    public long  nextOffset; //下一批次的偏移量    
	}
	最后简单做一个总结：
	IPartitionedTransactionalSpout提供了一种最简单的处理事务型应用的方法，持久化存储中要额外保存txid，它可能会出现spout卡住的问题。
	IOpaquePartitionedTransactionalSpout是最严谨的处理事务型应用的方法，但是使用它编写应用代码会更复杂一些，持久化存储中除了要额外
	保存txid之外，还要保存前一批次的业务计算结果。

	
	事务拓扑的配置及API：
	----------------------
	事务拓扑有两个重要的配置：
	zk的配置：默认情况下，事务拓扑会在相同的 zk 实例中保存状态，用于管理storm集群。可以通过设置 
	transactional.zookeeper.servers和 transactional.zookeeper.port 进行修改。
	一次允许的活跃batch数：必须限制一次处理的batch的数量。可以使用 topology.max.spout.pending，默认值为 1。
	
	构建拓扑：
	一般使用 TransactionalTopologyBuilder 来构建事务拓扑（集成到 Trident 中，已被弃用）。
	
	注意事项：
	当使用普通的 bolt 时，可以调用OutputCollector的fail方法来处理元组树的失败情况，其中元组是元组树的一个成员。由于事务拓扑隐藏了ack
	框架，提供一个不同的机制处理一个 batch 的失败，重发失败的 batch，并抛出一个 FailException 异常。不同于普通的异常，这只会引发普通
	batch 的重发，并不会导致进程的崩溃。
	 
	 
### Trident

	Trident 是 storm s还是计算的一个高层抽象。他可以让你无缝的混合使用高吞吐量（百万/s）、低延迟分布式查询处理状态流。
	Trident 有连接、聚合、分组、函数和过滤器等。此外，trident 增加了一些额外的原语，用在数据库或者持久化存储中进行增量处理。
	trident 有一致性和恰好一次的语义，所以 trident 拓扑很容易理解。
	
	
	
			
		
	
	










