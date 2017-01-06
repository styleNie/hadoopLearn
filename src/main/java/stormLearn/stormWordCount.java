package stormLearn;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/12/29.
 */
public class stormWordCount {
    public class WordCount implements IBasicBolt {
        private Map<String, Integer> _counts = new HashMap<String, Integer>();

        public void prepare(Map conf, TopologyContext context) {
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            int count;
            if (_counts.containsKey(word)) {
                count = _counts.get(word);
            } else {
                count = 0;
            }
            count++;
            _counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public class SplitSentence implements IBasicBolt {
        public void prepare(Map conf, TopologyContext context) {
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }

        public void cleanup() {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(1, new KestrelSpout("kestrel.backtype.com", 22133, "sentence_queue", new StringScheme()));
        builder.setBolt(2, new SplitSentence(), 10).shuffleGrouping(1);
        builder.setBolt(3, new WordCount(), 20).fieldsGrouping(2, new Fields("word"));
    }
}
