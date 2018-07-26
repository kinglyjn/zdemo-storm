package starter04.topology_rolling_top_words;

import org.apache.storm.utils.Time;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

/**
 * NthLastModifiedTimeTracker
 * @author kinglyjn
 * @date 2018年7月25日
 *
 */
public class NthLastModifiedTimeTracker {
	// 存放一个个时间点的的环形缓冲区
	private final CircularFifoBuffer lastModifiedTimesMillis;
	
	public NthLastModifiedTimeTracker(int numTimesToTrack) { //传递追踪次数，用以创建相应大小的唤醒缓冲区
		if (numTimesToTrack < 1) {
			throw new IllegalArgumentException("numTimesToTrack must be greater than zero (you requested " + numTimesToTrack + ")");
		}
		lastModifiedTimesMillis = new CircularFifoBuffer(numTimesToTrack);
		initLastModifiedTimesMillis();
	}
	private void initLastModifiedTimesMillis() {
		long nowCached = now();
		for (int i = 0; i < lastModifiedTimesMillis.maxSize(); i++) {
			lastModifiedTimesMillis.add(Long.valueOf(nowCached));
		}
	}
	private long now() {
		return Time.currentTimeMillis();
	}

	/** 获取当前时间与环形缓冲区最早时间的时间差 */
	public int secondsSinceOldestModification() {
		long modifiedTimeMillis = ((Long) lastModifiedTimesMillis.get()).longValue();
		return (int) ((now() - modifiedTimeMillis) / 1000);
	}

	/** 向环形缓冲区中增加当前时间点 */
	public void markAsModified() {
		updateLastModifiedTime();
	}

	/** 向环形缓冲区中增加当前时间点 */
	private void updateLastModifiedTime() {
		lastModifiedTimesMillis.add(now());
	}
}