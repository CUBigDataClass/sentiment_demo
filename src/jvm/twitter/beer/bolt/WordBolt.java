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
import java.util.HashMap;

public class WordBolt extends BaseRichBolt {
	// Word count for tweet text
	OutputCollector _collector;
  Map<String, Integer> counts_wine = new HashMap<String, Integer>();
  Map<String, Integer> counts_beer = new HashMap<String, Integer>();

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String word = tuple.getString(0);
    String type = tuple.getString(1);
    Integer count;
    Map<String, Integer> counts;

    if (type.equals("wine")){
      count = updateCount(counts_wine.get(word));
      counts_wine.put(word, count);

    }
    else{
      count = updateCount(counts_beer.get(word));
      counts_beer.put(word, count);
    }

  
    _collector.emit(tuple, new Values(word, count, type));
    _collector.ack(tuple);  

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "frequency", "classification"));
  }

  private Integer updateCount(Integer count){
    if (count == null){
      count = 0;
    } 

    count++;

    return count;
  }
}