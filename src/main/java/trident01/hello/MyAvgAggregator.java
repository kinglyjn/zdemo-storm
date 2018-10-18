package trident01.hello;

import java.io.Serializable;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * MyAvgAggregator
 * @author kinglyjn
 * @date 2018年8月14日
 *
 */
public class MyAvgAggregator extends BaseAggregator<MyAvgAggregator.State>{
	private static final long serialVersionUID = 1L;

	/**
	 * 用于记录本批次的聚合统计状态
	 * 
	 */
	static class State implements Serializable {
		private static final long serialVersionUID = 1L;
		Integer sum = 0;
		Integer count = 0;
	}

	@Override
	public State init(Object batchId, TridentCollector collector) {
		System.err.printf("thread=%s, object=MyAvgAggregator@%d: init, batchIdClass=%s, batchId=%s\n", Thread.currentThread().getName(), this.hashCode(), batchId.getClass().getSimpleName(), batchId);
		return new State();
	}

	@Override
	public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
		state.sum = tuple.getInteger(0) + state.sum;
		state.count++;
	}

	@Override
	public void complete(State state, TridentCollector collector) {
		Double avg = Double.valueOf(state.sum)/state.count;
		collector.emit(new Values(avg));
	}
}
