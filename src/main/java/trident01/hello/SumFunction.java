package trident01.hello;

import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class SumFunction extends BaseFunction {
	private static final long serialVersionUID = 1L;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		System.err.printf("thread=%s, object=SumFunction@%d: prepare\n", Thread.currentThread().getName(), this.hashCode());
	}
	
	
	/**
	 * Function#execute 计算头两个数的加和
	 * 
	 * 			function
	 * 1,2,3,4------------>1,2,3,4,3
	 * 5,6,7,8------------>5,6,7,8,11
	 * 9,10,11,12--------->9,10,11,12,19
	 * 13,14,15,16-------->13,14,15,16,27
	 * 
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Integer num0 = tuple.getInteger(0);
		Integer num1 = tuple.getInteger(1);
		Integer sum = num0 + num1;
		System.err.printf("thread=%s, object=SumFunction@%d: num0=%d, num1=%d, sum=%d\n", Thread.currentThread().getName(), this.hashCode(), num0, num1, sum);
		collector.emit(new Values(sum));
	}
	
}
