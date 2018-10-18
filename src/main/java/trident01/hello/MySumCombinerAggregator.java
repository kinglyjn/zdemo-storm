package trident01.hello;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import clojure.lang.Numbers;

/**
 * MySumCombinerAggregator
 * @author kinglyjn
 * @date 2018年8月14日
 *
 */
public class MySumCombinerAggregator implements CombinerAggregator<Number> {
	private static final long serialVersionUID = 1L;

	@Override
	public Number init(TridentTuple tuple) {
		Number num = (Number) tuple.getValue(0);
		System.err.printf("thread=%s, object=MySumCombinerAggregator@%d: init, num=%s\n", Thread.currentThread().getName(), this.hashCode(), num);
		return num;
	}

	@Override
	public Number combine(Number val1, Number val2) {
		Number sum = Numbers.add(val1, val2);
		System.err.printf("thread=%s, object=MySumCombinerAggregator@%d: combine, %s+%s=%s\n", Thread.currentThread().getName(), this.hashCode(), val1, val2, sum);
		return sum;
	}

	@Override
	public Number zero() {
		System.err.printf("thread=%s, object=MySumCombinerAggregator@%d: zero\n", Thread.currentThread().getName(), this.hashCode());
		return 0;
	}

}
