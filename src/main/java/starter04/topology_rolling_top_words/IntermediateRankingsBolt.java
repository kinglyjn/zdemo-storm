package starter04.topology_rolling_top_words;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;

/**
 * IntermediateRankingsBolt
 * @author kinglyjn
 * @date 2018年7月25日
 *
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {
	private static final long serialVersionUID = -1369800530256637409L;
	private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

	public IntermediateRankingsBolt() {
		super();
	}
	public IntermediateRankingsBolt(int topN) {
		super(topN);
	}
	public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
		super(topN, emitFrequencyInSeconds);
	}

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		Rankable rankable = RankableObjectWithFields.from(tuple);
		super.getRankings().updateWith(rankable);
	}
	
	@Override
	Logger getLogger() {
		return LOG;
	}
}
