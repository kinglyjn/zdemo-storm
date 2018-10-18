package trident01.hello;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * MySumAggregator
 * @author kinglyjn
 * @date 2018年8月14日
 *
 */
public class MySumReducerAggregator implements ReducerAggregator<Integer> {
	private static final long serialVersionUID = 1L;

	/**
	 * 用于初始化每一批次tuple数据的统计初始值
	 * 
	 */
	@Override
	public Integer init() {
		System.err.printf("thread=%s, object=MySumReducerAggregator@%d: init\n", Thread.currentThread().getName(), this.hashCode());
		return 0;
	}

	@Override
	public Integer reduce(Integer curr, TridentTuple tuple) {
		return curr + tuple.getInteger(0);
	}
}
