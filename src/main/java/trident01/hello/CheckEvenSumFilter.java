package trident01.hello;

import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;


public class CheckEvenSumFilter extends BaseFilter {
	private static final long serialVersionUID = 1L;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		System.err.printf("thread=%s, object=CheckEvenSumFilter@%d: prepare\n", Thread.currentThread().getName(), this.hashCode());
	}
	
	/**
	 * Filter#isKeep 检查头两个数的加和是否为偶数，不是的话该tuple就被过滤掉
	 * 
	 * 			 filter
	 * 1,1,3,4------------>1,1,3,4
	 * 3,1,4,1             x
	 * 2,3,4,1             x
	 * 2,4,1,2             2,4,1,2
	 * 
	 */
	@Override
	public boolean isKeep(TridentTuple tuple) {
		Integer num0 = tuple.getInteger(0);
		Integer num1 = tuple.getInteger(1);
		Integer sum = num0 + num1;
		boolean isKeep = sum%2==0 ? true : false;
		System.err.printf("thread=%s, object=CheckEvenSumFilter@%d: num0=%d, num1=%d, isKeep=%s\n", Thread.currentThread().getName(), this.hashCode(), num0, num1, isKeep);
		return isKeep;
	}

}
