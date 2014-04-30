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

public class ParseBolt extends BaseRichBolt {
	// Parses the sentence
	OutputCollector _collector;
 
  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String text = tuple.getString(1);
    String[] words = text.split("\\s+");
    String type = tuple.getString(3);

    for(int i=0; i < words.length; i++){
      _collector.emit(tuple, new Values(words[i], type));
      _collector.ack(tuple);
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