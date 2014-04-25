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

public class ClassifyBolt extends BaseRichBolt {
	// Extracts basic information from the tweet object
	OutputCollector _collector;
  String[] beerList = {"beer", "stout", "tripel", "dubbel", "porter", "pale ale", "IPA", "lager"};
  String[] wineList = {"wine", "pinot noir", "merlot", "cabernet sauvignon", "chianti", "malbec", "zinfandel", "chardonnay", "pinot grigio", "riesling"};

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
  	
    String tweet_id = tuple.getString(0);
    String text = tuple.getString(1);
    String lowercaseText = text.toLowerCase();
    String coordinates = tuple.getString(2);
    String classification = new String();

    // Determine classification
    for(String term : beerList){
      if(lowercaseText.contains(term)){
        classification = "beer";
      }
    }
    if(classification.isEmpty()){
      for(String term : wineList){
        if(lowercaseText.contains(term)){
          classification = "wine";
        }
      }
    }
      

    _collector.emit(tuple, new Values(tweet_id, text, coordinates, classification));
    _collector.ack(tuple);  

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweetID", "tweet", "coordinates", "classification"));
  }
}