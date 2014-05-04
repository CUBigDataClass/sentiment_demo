package twitter.beer.bolt;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContext;

import java.util.Map;
import java.util.HashSet;
import java.util.Arrays;

public class ParseBolt extends BaseRichBolt {
	// Parses the sentence
	OutputCollector _collector;
  HashSet<String> stopWords;
 
  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;

    String[] stopWordArray = {"just", "my", "i'll","!", "@","rt", "wine", "beer", "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "i", "you", "your","&amp", "too", "&", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"};
    stopWords = new HashSet<String>(Arrays.asList(stopWordArray));
  }

  @Override
  public void execute(Tuple tuple) {
    String text = tuple.getString(1);
    String[] words = text.split("\\s+");
    String type = tuple.getString(3);

    for(String word : words)
    {
      if(!stopWords.contains(word.toLowerCase()))
      {
        _collector.emit(tuple, new Values(word, type));
        _collector.ack(tuple);
      }
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "classification"));
  }

  private Integer updateCount(Integer count){
    if (count == null){
      count = 0;
    }

    count++;

    return count;
  }
}