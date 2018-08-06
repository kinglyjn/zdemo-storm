package test06.transactional_topo02_count_by_date;

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
 * OrderSpout
 * @author kinglyjn
 * @date 2018年7月27日
 *
 */
public class OrderSpout implements ITransactionalSpout<OrderMetaData>{
	private static final long serialVersionUID = 1L;
	private static final int BATCH_SIZE = 15;
	private static final Map<Integer, String> ORDER_DB_MAP;
	static {
		ORDER_DB_MAP = new HashMap<>();
		for (int i = 0; i < 100; i++) {
			if (i < 20) {
				ORDER_DB_MAP.put(i, "2018-07-25 order-" + i); //2018-07-25 20单
			} else if (i < 50) {
				ORDER_DB_MAP.put(i, "2018-07-26 order-" + i); //2018-07-25 30单
			} else if (i < 100) {
				ORDER_DB_MAP.put(i, "2018-07-27 order-" + i); //2018-07-26 50单
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "order"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public Coordinator<OrderMetaData> getCoordinator(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OrderCoordinator();
	}

	@Override
	public Emitter<OrderMetaData> getEmitter(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new OrderEmitter();
	}
	
	
	/**
	 * OrderCoordinator 订单事务协同器
	 * @author kinglyjn
	 * @date 2018年7月27日
	 *
	 */
	static class OrderCoordinator implements Coordinator<OrderMetaData> {
		@Override
		public OrderMetaData initializeTransaction(BigInteger txid, OrderMetaData prevMetadata) { //初始化事务
			OrderMetaData metaData = null;
			if (prevMetadata==null) {
				metaData = new OrderMetaData(0, BATCH_SIZE);
			} else {
				int beginPonit = prevMetadata.getBeginPonit();
				int batchSize = prevMetadata.getBatchSize();
				metaData = new OrderMetaData(beginPonit+batchSize, batchSize);
			}
			System.err.println("OrderCoordinator@" + this.hashCode() + ": 启动一个事务, txid=" + txid + ", metaData=" + metaData);
			return metaData;
		}

		@Override
		public boolean isReady() {
			Utils.sleep(2000); //
			return true;
		}

		@Override
		public void close() {
			//
		}
	}
	
	
	/**
	 * OrderEmitter 订单事务发射器
	 * @author kinglyjn
	 * @date 2018年7月27日
	 *
	 */
	static class OrderEmitter implements Emitter<OrderMetaData> {
		@Override
		public void emitBatch(TransactionAttempt tx, OrderMetaData coordinatorMeta, BatchOutputCollector collector) {
			int beginPonit = coordinatorMeta.getBeginPonit();
			int batchSize = coordinatorMeta.getBatchSize();
			for (int i = beginPonit; i < beginPonit+batchSize; i++) {
				collector.emit(new Values(tx, ORDER_DB_MAP.get(i)));
			}
		}

		@Override
		public void cleanupBefore(BigInteger txid) {
			//
		}

		@Override
		public void close() {
			//
		}
	}
	
}
