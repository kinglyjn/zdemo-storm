package trident01.hello;

import java.io.Serializable;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * MySumAggregator
 * @author kinglyjn
 * @date 2018年8月14日
 *
 */
public class MySumAggregator extends BaseAggregator<MySumAggregator.State> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * State 
	 * 用于存放每个批次tuple数据的统计结果
	 *
	 */
	static class State implements Serializable {
		private static final long serialVersionUID = 1L;
		Integer count = 0;
	}

	@Override
	public State init(Object batchId, TridentCollector collector) { //batchId 的类型为 TransactionAttempt[_txid,_attemptId]
		System.err.printf("thread=%s, object=MySumAggregator@%d: init, batchIdClass=%s, batchId=%s\n", Thread.currentThread().getName(), this.hashCode(), batchId.getClass().getSimpleName(), batchId);
		return new State();
	}

	@Override
	public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
		state.count = tuple.getInteger(0) + state.count;
	}

	@Override
	public void complete(State state, TridentCollector collector) {
		System.err.printf("thread=%s, object=MySumAggregator@%d: complete\n", Thread.currentThread().getName(), this.hashCode());
		collector.emit(new Values(state.count));
	}
}
