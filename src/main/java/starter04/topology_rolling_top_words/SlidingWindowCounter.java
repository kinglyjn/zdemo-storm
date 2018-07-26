package starter04.topology_rolling_top_words;

import java.io.Serializable;
import java.util.Map;

public final class SlidingWindowCounter<T> implements Serializable {
	private static final long serialVersionUID = -2645063988768785810L;
	// 通过卡槽计数器作为底层支持实现滑动窗口计数
	private SlotBasedCounter<T> objCounter; 
	// 头槽
	private int headSlot;
	// 尾槽
	private int tailSlot;
	// 窗口卡槽个数（卡槽环形数组的长度）
	private int windowLengthInSlots;

	public SlidingWindowCounter(int windowLengthInSlots) {
		// 窗口卡槽的个数必须在2个或2个以上
		if (windowLengthInSlots < 2) {
			throw new IllegalArgumentException("Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
		}
		this.windowLengthInSlots = windowLengthInSlots;
		this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);
		this.headSlot = 0;
		this.tailSlot = slotAfter(headSlot);
	}

	/** 当前头槽下该对象计数递增 */
	public void incrementCount(T obj) {
		objCounter.incrementCount(obj, headSlot);
	}

	/** 获取所有对象在当前窗口的计数总数，并且头槽和尾槽的位置同时后移一个单位 */
	public Map<T, Long> getCountsThenAdvanceWindow() {
		Map<T, Long> counts = objCounter.getCounts();
		objCounter.wipeZeros();
		objCounter.wipeSlot(tailSlot);
		advanceHead();
		return counts;
	}

	/** 头槽和尾槽的位置同时后移一个单位 */
	private void advanceHead() {
		headSlot = tailSlot;
		tailSlot = slotAfter(tailSlot);
	}

	/** 获取环形数组某个卡槽后面卡槽的下标 */
	private int slotAfter(int slot) {
		return (slot + 1) % windowLengthInSlots;
	}
	
}
