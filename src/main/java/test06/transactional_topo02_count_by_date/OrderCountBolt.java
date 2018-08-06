package test06.transactional_topo02_count_by_date;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * OrderCountBolt 订单事务计数器Bolt
 * @author kinglyjn
 * @date 2018年7月27日
 *
 */
public class OrderCountBolt implements IBatchBolt<TransactionAttempt> {
	private static final long serialVersionUID = 1L;
	private BatchOutputCollector collector;
	private TransactionAttempt tx;
	private Map<String, Integer> dateCountMap;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "dateCountMap"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.collector = collector;
		this.tx = id;
		this.dateCountMap = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String order = tuple.getString(1);
		if (order==null) {
			return;
		}
		String[] splits = order.split(" ");
		if(splits!=null && splits.length==2) {
			String date = splits[0].trim();
			Integer count = dateCountMap.get(date);
			if (count==null) {
				count = 0;
			}
			dateCountMap.put(date, ++count);
		}
	}

	@Override
	public void finishBatch() {
		collector.emit(new Values(tx, dateCountMap));
		System.err.println("OrderCountBolt@" + this.hashCode() + ": txid=" + tx.getTransactionId() + ", dateCountMap=" + dateCountMap);
	}
}
