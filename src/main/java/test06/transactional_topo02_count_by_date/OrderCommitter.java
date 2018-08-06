package test06.transactional_topo02_count_by_date;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

/**
 * OrderCommitter 订单事务提交器
 * @author kinglyjn
 * @date 2018年7月27日
 *
 */
public class OrderCommitter extends BaseTransactionalBolt implements ICommitter {
	private static final long serialVersionUID = 1L;
	private static final Map<String, DBValue> SINK_DB_MAP = new HashMap<>();
	
	private TransactionAttempt tx;
	private Map<String, Integer> currentTxCountMap;
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.tx = id;
		this.currentTxCountMap = new HashMap<>();
	}

	private void mergeCountMap(Map<?,?> countMap) {
		if (countMap==null) {
			return;
		}
		for (Object key : countMap.keySet()) {
			String date = (String) key;
			Integer count = (Integer) countMap.get(key);
			
			Integer currentCount = currentTxCountMap.get(date);
			if (currentCount==null) {
				currentCount = 0;
			}
			currentCount += count;
			currentTxCountMap.put(date, currentCount);
		}
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void execute(Tuple tuple) {
		Map dateCountMap = (Map) tuple.getValue(1);
		mergeCountMap(dateCountMap);
	}

	@Override
	public void finishBatch() {
		for (String date : currentTxCountMap.keySet()) {
			Integer count = currentTxCountMap.get(date);
			DBValue currentDBValue = SINK_DB_MAP.get(date);
			if (currentDBValue==null) {
				SINK_DB_MAP.put(date, new DBValue(date, this.tx.getTransactionId(), count));
			} else if (!this.tx.getTransactionId().equals(currentDBValue.tx)) {
				currentDBValue.tx = this.tx.getTransactionId();
				currentDBValue.count += count;
			}
		}
		System.err.println("OrderCommitter@" + this.hashCode() + "#finishBatch: txid=" + this.tx.getTransactionId() + ", sinkDBMap=" + SINK_DB_MAP + "\n");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//
	}

	/**
	 * DBValue 落地数据的数据结构
	 * @author kinglyjn
	 * @date 2018年7月27日
	 *
	 */
	static class DBValue {
		String date;
		BigInteger tx;
		Integer count;
		
		public DBValue(String date, BigInteger tx, Integer count) {
			this.date = date;
			this.tx = tx;
			this.count = count;
		}

		@Override
		public String toString() {
			return "[date=" + date + ", tx=" + tx + ", count=" + count + "]";
		}
	}
	
}
