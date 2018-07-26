package starter04.topology_rolling_top_words;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 工作原理：<br>
 * SlidingwindowCounter在使用它时，会指定一个headSlot，这样某一个时刻，所有的obj的计数都会写入某个下标相同的slot里。<br>
 * SlotBasedConter实际上是维护一个Map<T, long[]> objToCounts对象。<br><br>
 * 
 * SlotBasedCounter
 * @author kinglyjn
 * @date 2018年7月25日
 *
 * @param <T>
 */
public final class SlotBasedCounter<T> implements Serializable {
	private static final long serialVersionUID = 4858185737378394432L;
	private final Map<T, long[]> objToCounts = new HashMap<T, long[]>();
	private final int numSlots;	//追踪槽个数

	public SlotBasedCounter(int numSlots) {
		if (numSlots <= 0) {
			throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
		}
		this.numSlots = numSlots;
	}

	/** 在当前追踪卡槽下将该对象的计数加1 */
	public void incrementCount(T obj, int slot) {
		long[] counts = objToCounts.get(obj);
		if (counts == null) {
			counts = new long[this.numSlots];
			objToCounts.put(obj, counts);
		}
		counts[slot]++;
	}

	/** 获取当前追踪卡槽下该对象的计数 */
	public long getCount(T obj, int slot) {
		long[] counts = objToCounts.get(obj);
		if (counts == null) {
			return 0;
		} else {
			return counts[slot];
		}
	}
	
	/** 获取所有对象在当前计数器的计数总数 */
	public Map<T, Long> getCounts() {
		Map<T, Long> result = new HashMap<T, Long>();
		for (T obj : objToCounts.keySet()) {
			result.put(obj, computeTotalCount(obj));
		}
		return result;
	}
	
	/** 计算某个对象在当前计数器的计数总数 */
	private long computeTotalCount(T obj) {
		long[] curr = objToCounts.get(obj);
		long total = 0;
		for (long l : curr) {
			total += l;
		}
		return total;
	}

	/** 将当前卡槽的所有计数清零 */
	public void wipeSlot(int slot) {
		for (T obj : objToCounts.keySet()) {
			resetSlotCountToZero(obj, slot);
		}
	}
	private void resetSlotCountToZero(T obj, int slot) {
		long[] counts = objToCounts.get(obj);
		counts[slot] = 0;
	}
	
	/** 判断该对象的在当前计数器的计数是否为0 */
	private boolean shouldBeRemovedFromCounter(T obj) {
		return computeTotalCount(obj) == 0;
	}

	/** 将当前窗口计数为0的对象从计数器中清除，以释放内存空间 */
	public void wipeZeros() {
		Set<T> objToBeRemoved = new HashSet<T>();
		for (T obj : objToCounts.keySet()) {
			if (shouldBeRemovedFromCounter(obj)) {
				objToBeRemoved.add(obj);
			}
		}
		for (T obj : objToBeRemoved) {
			objToCounts.remove(obj);
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (T obj : objToCounts.keySet()) {
			long[] countArray = objToCounts.get(obj);
			sb.append(obj + ": " + Arrays.toString(countArray) + "\n");
		}
		return sb.toString();
	}
	
	 /*
	 * main test
	 * 
	 */
	public static void main(String[] args) {
		SlotBasedCounter<Object> counter = new SlotBasedCounter<>(5);
		Integer obj1 = new Integer(1);
		Integer obj2 = new Integer(2);
		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < 10; j++) {
				counter.incrementCount(obj1, i);
				counter.incrementCount(obj2, i);
			}
			System.out.println(counter);
		}
	}
}
