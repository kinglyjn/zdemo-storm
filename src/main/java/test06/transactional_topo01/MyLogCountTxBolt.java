package test06.transactional_topo01;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * MyLogCountBolt
 * @author kinglyjn
 * @date 2018年7月26日
 *
 */
public class MyLogCountTxBolt extends BaseTransactionalBolt {
	private static final long serialVersionUID = 1L;
	private BatchOutputCollector collector;
	private TransactionAttempt tx;
	private int count = 0;
	
	/**
	 * Storm会为每一个事务创建线程{bolt_parallelism}个线程安全的MyLogCountBolt实例
	 * 
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.collector = collector;
		this.tx = id;
		System.err.println("MyLogCountTxBolt@" + this.hashCode() + "#prepare, tx.txid=" + tx.getTransactionId() + ", tx.attemptId=" + tx.getAttemptId());
		//MyLogCountTxBolt@958276434#prepare, tx.txid=1, tx.attemptId=2243358004186960407
		//MyLogCountTxBolt@1443189179#prepare, tx.txid=1, tx.attemptId=2243358004186960407
		//MyLogCountTxBolt@783765674#prepare, tx.txid=1, tx.attemptId=2243358004186960407
		
		//MyLogCountTxBolt@1489155339#prepare, tx.txid=2, tx.attemptId=-1516662102336376242
		//MyLogCountTxBolt@544254732#prepare, tx.txid=2, tx.attemptId=-1516662102336376242
		//MyLogCountTxBolt@1552302839#prepare, tx.txid=2, tx.attemptId=-1516662102336376242
		
		//MyLogCountTxBolt@609172442#prepare, tx.txid=3, tx.attemptId=3121103668892972913
		//MyLogCountTxBolt@340455046#prepare, tx.txid=3, tx.attemptId=3121103668892972913
		//MyLogCountTxBolt@1560099531#prepare, tx.txid=3, tx.attemptId=3121103668892972913
		
		//MyLogCountTxBolt@436191024#prepare, tx.txid=4, tx.attemptId=7991583790903691476
		//MyLogCountTxBolt@26300611#prepare, tx.txid=4, tx.attemptId=7991583790903691476
		//MyLogCountTxBolt@1632477495#prepare, tx.txid=4, tx.attemptId=7991583790903691476
	}

	@Override
	public void execute(Tuple tuple) {
		//TransactionAttempt tx = (TransactionAttempt) tuple.getValue(0);
		String log = (String) tuple.getValue(1);
		if (!StringUtils.isEmpty(log)) {
			count++;
			//下面的代码会让事务拓扑立马死掉，实验证明程序没有走到下一个bolt（此例为MyLogTxCommitter）
			//if (count==5) Integer.parseInt("aaaa"); 
		}
	}

	@Override
	public void finishBatch() {
		collector.emit(new Values(tx, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "count"));
	}

}
