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
import org.json.JSONObject;

public class ExtractBolt extends BaseRichBolt {
	// Extracts basic information from the tweet object
	OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	JSONObject tweetJSON = new JSONObject(tuple.getString(0));
    	String tweet_id = tweetJSON.getString("id_str").trim();
    	String text = tweetJSON.getString("text").trim();
    	String language = tweetJSON.getString("lang");
    	
    	if(language.equals("en")){
    		_collector.emit(tuple, new Values(tweet_id, text));
      		_collector.ack(tuple);
    	}
      	else{
      		_collector.emit(tuple, new Values(tweet_id, language));
      		_collector.ack(tuple);
      	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("tweetID", "tweet"));
    }
}