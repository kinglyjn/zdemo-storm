package starter04.topology_rolling_top_words;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;

public final class TotalRankingsBolt extends AbstractRankerBolt {
	private static final long serialVersionUID = -8447525895532302198L;
	private static final Logger LOG = Logger.getLogger(TotalRankingsBolt.class);

	public TotalRankingsBolt() {
		super();
	}

	public TotalRankingsBolt(int topN) {
		super(topN);
	}

	public TotalRankingsBolt(int topN, int emitFrequencyInSeconds) {
		super(topN, emitFrequencyInSeconds);
	}

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
		super.getRankings().updateWith(rankingsToBeMerged);
		super.getRankings().pruneZeroCounts();
	}

	@Override
	Logger getLogger() {
		return LOG;
	}
}
