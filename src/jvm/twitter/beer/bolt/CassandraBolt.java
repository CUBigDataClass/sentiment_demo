package twitter.beer.bolt;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter.beer.CassandraClient;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContext;
import java.util.Map;

public class CassandraBolt extends BaseRichBolt{
	OutputCollector _collector;
	CassandraClient client;
	
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      this.client = new CassandraClient();
      // Start client
      this.client.connect("127.0.0.1");
    }

    @Override
    public void execute(Tuple tuple) {
    	Integer tweet_id = tuple.getInteger(0);
    	this.client.loadData("INSERT INTO twitter.tweet (tweet_id, tweet) VALUES (" + tweet_id + ",'" + tuple.getValue(1) + "');");
    	//this.client.querySchema("SELECT * from twitter.tweet WHERE tweet_id = " + tweet_id + ");");
    	_collector.emit(tuple, new Values(tweet_id));
    	_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("cassandraOutput"));
  	}

  	@Override
  	public void  cleanup(){
  		this.client.close();
  	}

}