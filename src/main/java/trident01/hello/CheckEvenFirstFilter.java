package trident01.hello;

import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

public class CheckEvenFirstFilter extends BaseFilter {
	private static final long serialVersionUID = 1L;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		System.err.printf("thread=%s, object=CheckEvenFirstFilter@%d: prepare\n", Thread.currentThread().getName(), this.hashCode());
	}
	
	/**
	 * 检查tuple第一个数是否为偶数，不是的话过滤掉
	 * 
	 */
	@Override
	public boolean isKeep(TridentTuple tuple) {
		Integer num0 = tuple.getInteger(0);
		boolean isKeep = num0%2==0 ? true : false;
		System.out.printf("thread=%s, object=CheckEvenFirstFilter@%d: num0=%d, isKeep=%s\n", Thread.currentThread().getName(), this.hashCode(), num0, isKeep);
		return isKeep;
	}
}
