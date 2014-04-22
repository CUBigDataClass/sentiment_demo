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
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraBolt extends BaseRichBolt{
	OutputCollector _collector;
	CassandraClient client;
	
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      this.client = new CassandraClient();
      // Start client
      this.client.connect("127.0.0.1");
      //this.client.connect("54.186.242.244");
    }

    @Override
    public void execute(Tuple tuple) {
    	String tweet_id = tuple.getString(0);
      String text = tuple.getString(1);
      String coordinates = tuple.getString(2);
      String classification = tuple.getString(3);

      String insertion = QueryBuilder.insertInto("twitter", "tweet").value("tweet_id", tweet_id).value("tweet", text).value("coordinates", coordinates).value("classification", classification).toString();
    	this.client.loadData(insertion);
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