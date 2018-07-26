package starter02.topology_wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * WordCountTopology
 * @author kinglyjn
 * @date 2018年7月24日
 *
 */
public class WordCountTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
       
		builder.setSpout("spout", new RandomSentenceSpout(), 5);
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
        conf.setDebug(true);
        conf.setNumWorkers(3);
        
        String topologyName = "wordcount-topology";
        if (args != null && args.length > 0) {
            topologyName = args[0];
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
        	HashMap<String, String> env = new HashMap<>();
        	env.put(Config.STORM_LOCAL_DIR, System.getProperty("user.dir")+"/src/main/resources/");
        	conf.setEnvironment(env);
        	
        	LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, conf, builder.createTopology());
        }
	}
	
	/**
	 * RandomSentenceSpout
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class RandomSentenceSpout extends BaseRichSpout {
		private static final long serialVersionUID = 1L;
		private SpoutOutputCollector _collector;
	    private Random _rand;
	    
		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
	        _rand = new Random();
		}

		@Override
		public void nextTuple() {
			Utils.sleep(100); //
	        String[] sentences = new String[]{
	        	"the cow jumped over the moon", 
	        	"an apple a day keeps the doctor away",
	        	"four score and seven years ago",
	        	"snow white and the seven dwarfs",
	        	"i am at two with nature"
	        };
	        final String sentence = sentences[_rand.nextInt(sentences.length)];
	        System.err.println("Emitting tuple: " + sentence);
	        _collector.emit(new Values(sentence));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
	
	/**
	 * SplitSentence
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	static class SplitSentence extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String sentence = input.getString(0);
			String[] splits = sentence.split(" ");
			
			if (splits==null || splits.length==0) {
				return;
			}
			for (String word : splits) {
				word = word.trim();
				collector.emit(new Values(word));
			}
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
	
	/**
	 * WordCount
	 * @author kinglyjn
	 * @date 2018年7月24日
	 *
	 */
	public static class WordCount extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;
		Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            counts.put(word, ++count);
            collector.emit(new Values(word, count)); // 继续发射tuple以打印测试结果~
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
	
}
