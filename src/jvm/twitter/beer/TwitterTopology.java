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
import twitter.beer.bolt.ExtractBolt;
import java.util.Map;

// A basic topology that reads from the Twitter stream and does basic processing on the data
public class TwitterTopology {

	public static void main(String[] args) throws Exception{
		// Create a new Topology
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("tweetSpout", new TwitterSpout(), 10);
		builder.setBolt("tweetVal", new ExtractBolt(), 3).shuffleGrouping("tweetSpout");
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