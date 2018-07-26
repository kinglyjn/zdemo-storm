package test06.transactional_topo01;

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
 * MyLogCommitter
 * @author kinglyjn
 * @date 2018年7月26日
 *
 */
public class MyLogTxCommitter extends BaseTransactionalBolt implements ICommitter {
	private static final long serialVersionUID = 1L;
	private TransactionAttempt tx;
	private int sum = 0; 
	
	// 使用静态变量 SINK_DB_MAP 模拟落地数据库
	private static final Map<String, DBValue> SINK_DB_MAP = new HashMap<>();
	private static final String TOTAL_COUNT_KEY = "total_count";
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.tx = id;
	}

	@Override
	public void execute(Tuple tuple) {
		//TransactionAttempt tx = (TransactionAttempt) tuple.getValue(0);
		Integer count = tuple.getInteger(1);
		sum += count;
	}

	@Override
	public void finishBatch() {
		DBValue sinkValue = SINK_DB_MAP.get(TOTAL_COUNT_KEY);
		
		// 更新数据库
		if (sinkValue==null) { //当sinkValue为空
			sinkValue = new DBValue(tx.getTransactionId(), sum);
		} else if (!tx.getTransactionId().equals(sinkValue.txid)) { //当前事务id与数据库事务id不相同
			int totalCount = sinkValue.totalCount + sum;
			sinkValue = new DBValue(tx.getTransactionId(), totalCount);
		}
		SINK_DB_MAP.put(TOTAL_COUNT_KEY, sinkValue);
		
		// 打印日志
		System.err.println("MyLogTxCommitter@"+ this.hashCode() +"#finishBatch, tx.txid=" + tx.getTransactionId() + ", tx.attemptId=" + tx.getAttemptId() + ", totalCount=" + sinkValue.totalCount);
		//MyLogTxCommitter@1662516567#finishBatch, tx.txid=1, tx.attemptId=2243358004186960407, totalCount=20
		//MyLogTxCommitter@1842351941#finishBatch, tx.txid=2, tx.attemptId=-1516662102336376242, totalCount=40
		//MyLogTxCommitter@1777309223#finishBatch, tx.txid=3, tx.attemptId=3121103668892972913, totalCount=60
		//MyLogTxCommitter@753730942#finishBatch, tx.txid=4, tx.attemptId=7991583790903691476, totalCount=80
		//...
		//MyLogTxCommitter@491749282#finishBatch, tx.txid=10, tx.attemptId=-5478323801121788952, totalCount=200
		//MyLogTxCommitter@331657569#finishBatch, tx.txid=11, tx.attemptId=-7803263427952461348, totalCount=200
		//...
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//
	}
	
	
	/**
	 * DBValue 落地数据的数据类型
	 * @author kinglyjn
	 * @date 2018年7月27日
	 *
	 */
	static class DBValue {
		BigInteger txid; // 事务id
		int totalCount;	 // 当前统计结果
		
		public DBValue(BigInteger txid, int totalCount) {
			this.txid = txid;
			this.totalCount = totalCount;
		}
	}
}
