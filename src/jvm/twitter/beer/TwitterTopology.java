package twitter.beer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter.beer.spout.TwitterSpout;
import twitter.beer.bolt.CassandraBolt;
import java.util.Map;

// A basic topology that reads from the Twitter stream and does basic processing on the data
public class TwitterTopology {

	public static class ExtractBolt extends BaseRichBolt {
		// Extracts basic information from the tweet object
		OutputCollector _collector;

	    @Override
	    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	      _collector = collector;
	    }

	    @Override
	    public void execute(Tuple tuple) {
	      _collector.emit(tuple, new Values(tuple.getString(0) + "mod"));
	      _collector.ack(tuple);
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word"));
	    }
	}

	public static void main(String[] args) throws Exception{
		// Create a new Topology
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("tweetId", new TwitterSpout(), 10);
		builder.setBolt("tweetVal", new ExtractBolt(), 3).shuffleGrouping("tweetId");
		builder.setBolt("cassandra", new CassandraBolt(), 3).shuffleGrouping("tweetVal");

		// Create new config
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);

	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    else {

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("test", conf, builder.createTopology());
	      Utils.sleep(10000);
	      cluster.killTopology("test");
	      cluster.shutdown();
	    }
	}

}