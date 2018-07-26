
直接使用 storm-start 的示例组件：

1. 依赖
	<!-- storm -->
	<dependency>
		<groupId>org.apache.storm</groupId>
		<artifactId>storm-core</artifactId>
		<version>${storm.version}</version>
		<scope>provided</scope>
	</dependency>
	<dependency>
	    <groupId>org.apache.storm</groupId>
	    <artifactId>storm-starter</artifactId>
	    <version>${storm.version}</version>
	    <scope>provided</scope>
	</dependency>
	

2. 服务端 ${storm_home}/lib 中导入 storm-starter.jar
3. storm jar zdemo-storm-starter-0.0.1-SNAPSHOT.jar test05.topology_rolling_top_words2.RollingTopWordsTopology RollingTopWordsTopology

	
