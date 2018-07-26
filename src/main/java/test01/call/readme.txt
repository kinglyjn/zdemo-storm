Storm 的配置优先级为:
	defaults.yaml < storm.yaml < 拓扑配置 < 内置型组件信息配置 < 外置型组件信息配置


storm 的worker、executor、task
	worker:
		说明：拓扑在集群中运行所需要的工作进程数，一个worker只能绑定到一个topology中
		配置选项：TOPOLOGY_WORKERS
		在代码中如何使用（示例）：
		Config#setNumWorkers
		注意：在topology运行期间可以动态修改
		
	executor:
		说明：每个组件需要的执行线程数，每个执行线程只能执行一个组件
		配置选项：（没有拓扑级的通用配置项）
		在代码中如何使用（示例）：
		TopologyBuilder#setSpout()
		TopologyBuilder#setBolt()
		注意：从 Storm 0.8 开始 parallelism_hint 参数代表 executor 的数量，而不是 task 的数量
			在topology运行期间可以动态修改
	task:
		说明：每个组件的一个实例成为一个task，如果设置某个组件的parallelism_hint为3，task数为6，则每个executor线程中有两个任务实例
		配置选项：TOPOLOGY_TASKS
		在代码中如何使用（示例）：
		ComponentConfigurationDeclarer#setNumTasks()	
		注意：在topology运行期间不能动态修改
		
	某个topology的并发度：
		[spout's task] + [all bolt's task]
		
		
再平衡的两种方式：
	rebalance的过程是先钝化（deactive）topology，然后重新分配资源，返回到之前的resume处理。
	WEB UI：
	CLI：
		$ storm rebalance topologyname -n numWorkers -e spoutname=numExecutors -e bolt1name=numExecutors ...
	
	
		
	
	
	