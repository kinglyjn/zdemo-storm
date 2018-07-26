package test06.transactional_topo01;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * MyLogSpout
 * @author kinglyjn
 * @date 2018年7月26日
 *
 */
public class MyLogTxSpout implements ITransactionalSpout<MyMetaData> {
	private static final long serialVersionUID = 1L;
	// 使用DB_MAP系统产生的日志
	private static final Map<Integer, String> DB_MAP;
	private static int BATCH_SIZE = 10;
	static {
		DB_MAP = new HashMap<>();
		for (int i = 0; i < 100; i++) {
			DB_MAP.put(i, "log_" + i);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * 只会执行一次，一个事务拓扑中只有一个Coordinator
	 * MyLogTxSpout@2045127740#getCoordinator
	 * 
	 */
	@Override
	public Coordinator<MyMetaData> getCoordinator(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		System.err.println("MyLogTxSpout@" + this.hashCode() + "#getCoordinator");
		return new MyCoordinator();
	}
	
	/**
	 * 执行{spout_parallelism}次，产生{spout_parallelism}个Emitter
	 * MyLogTxSpout@1111539867#getEmitter
	 * MyLogTxSpout@1039714472#getEmitter
	 * 
	 */
	@Override
	public Emitter<MyMetaData> getEmitter(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		System.err.println("MyLogTxSpout@" + this.hashCode() + "#getEmitter");
		return new MyEmitter();
	}
	
	
	/**
	 * MyCoordinator
	 * @author kinglyjn
	 * @date 2018年7月26日
	 *
	 */
	static class MyCoordinator implements Coordinator<MyMetaData> {
		@Override
		public MyMetaData initializeTransaction(BigInteger txid, MyMetaData prevMetadata) { //txid为事务的id，默认从1开始
			MyMetaData metaData = null;
			if (prevMetadata==null) { // 第一次开启一个事务
				metaData = new MyMetaData(0, BATCH_SIZE);
			} else {
				metaData = new MyMetaData(prevMetadata.getBeginPonit() + prevMetadata.getBatchSize(), BATCH_SIZE);
			}
			System.err.println("MyCoordinator@" + this.hashCode() + ": 启动一个事务: " + metaData);
			//MyCoordinator@2110191065: 启动一个事务: MyMetaData [beginPonit=0, batchSize=10]
			//MyCoordinator@2110191065: 启动一个事务: MyMetaData [beginPonit=10, batchSize=10]
			//MyCoordinator@2110191065: 启动一个事务: MyMetaData [beginPonit=20, batchSize=10]
			//MyCoordinator@2110191065: 启动一个事务: MyMetaData [beginPonit=30, batchSize=10]
			//MyCoordinator@2110191065: 启动一个事务: MyMetaData [beginPonit=40, batchSize=10]
			return metaData;
		}

		@Override
		public boolean isReady() {
			Utils.sleep(2000); //
			return true;
		}

		@Override
		public void close() {
			// 释放资源
		}
		
	}
	
	/**
	 * MyEmitter
	 * @author kinglyjn
	 * @date 2018年7月26日
	 *
	 */
	static class MyEmitter implements Emitter<MyMetaData> {
		@Override
		public void emitBatch(TransactionAttempt tx, MyMetaData coordinatorMeta, BatchOutputCollector collector) {
			int beginPonit = coordinatorMeta.getBeginPonit();
			int batchSize = coordinatorMeta.getBatchSize();
			for (int i = beginPonit; i < beginPonit+batchSize; i++) {
				collector.emit(new Values(tx, DB_MAP.get(i)));
			}
		}

		@Override
		public void cleanupBefore(BigInteger txid) {
			//
		}

		@Override
		public void close() {
			// 释放资源
		}
	}
}
