package trident01.hello;

import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import clojure.lang.Numbers;

public class PrinterFunction extends BaseFunction {
	private static final long serialVersionUID = 1L;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		System.err.printf("thread=%s, object=PrinterFunction@%d: prepare\n", Thread.currentThread().getName(), this.hashCode());
	}
	
	/**
	 * 打印最终结果function
	 * 
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if (tuple.size()==1) {
			Object value = tuple.get(0);
			System.err.printf("thread=%s, object=PrinterFunction@%d: value=%d\n", Thread.currentThread().getName(), this.hashCode(), value);
		} else if (tuple.size()==2) {
			Object num0 = tuple.getValue(0);
			Object num1 = tuple.getValue(1);
			Number sumResult = 0;
			if (num0 instanceof Number && num1 instanceof Number) {
				sumResult = Numbers.add(num0, num1);
			}
			System.err.println("thread="+Thread.currentThread().getName()+", object=PrinterFunction@"+this.hashCode()+": num0="+num0+", num1="+num1+", sumResult="+sumResult);
		}
	}
}
