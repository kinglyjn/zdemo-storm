package starter04.topology_rolling_top_words;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

/**
 * AbstractRankerBolt
 * @author kinglyjn
 * @date 2018年7月25日
 *
 */
public abstract class AbstractRankerBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4931640198501530202L;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;	// 默认2秒钟的 tick tuple
	private static final int DEFAULT_COUNT = 10;	// 获取前排名最高的10个
	private final int emitFrequencyInSeconds;		// 实际触发间隔
	private final int count;						// 实际获取排名最高的前N个
	private final Rankings rankings;				// 排名的数据结构

	public AbstractRankerBolt() {
		this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}
	public AbstractRankerBolt(int topN) {
		this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}
	public AbstractRankerBolt(int topN, int emitFrequencyInSeconds) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
		}
		if (emitFrequencyInSeconds < 1) {
			throw new IllegalArgumentException(
					"The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
		}
		count = topN;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		rankings = new Rankings(count);
	}
	protected Rankings getRankings() {
		return rankings;
	}

	abstract Logger getLogger();
	abstract void updateRankingsWithTuple(Tuple tuple);
	private void emitRankings(BasicOutputCollector collector) {
		collector.emit(new Values(rankings.copy()));
		getLogger().debug("Rankings: " + rankings);
	}
	
	/**
	 * This method functions as a template method (design pattern).
	 * 
	 */
	@Override
	public final void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleUtils.isTick(tuple)) {
			getLogger().debug("Received tick tuple, triggering emit of current rankings");
			emitRankings(collector);
		} else {
			updateRankingsWithTuple(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rankings"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}
}
