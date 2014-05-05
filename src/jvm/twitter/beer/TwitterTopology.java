package twitter.beer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import twitter.beer.spout.TwitterSpout;
import twitter.beer.bolt.ClassifyBolt;
import twitter.beer.bolt.CassandraBolt;
import twitter.beer.bolt.ExtractBolt;
import twitter.beer.bolt.NodeBolt;
import twitter.beer.bolt.WordBolt;
import twitter.beer.bolt.WordNodeBolt;
import twitter.beer.bolt.ParseBolt;

import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.BrokerHosts;
import storm.kafka.Broker;
import storm.kafka.StaticHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.KafkaSpout;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;


// A basic topology that reads from the Twitter stream and does basic processing on the data
public class TwitterTopology {

	public static void main(String[] args) throws Exception{
		// Create a new Topology
		TopologyBuilder builder = new TopologyBuilder();

		// Set up KafkaSpout
		GlobalPartitionInformation hostsAndPartitions = new GlobalPartitionInformation();
		hostsAndPartitions.addPartition(0, new Broker("54.187.141.69", 9092));
		BrokerHosts brokerHosts = new StaticHosts(hostsAndPartitions);

		SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, "tweets", "", "storm");
		kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaSpoutConfig.forceFromStart = true;

	
		//builder.setSpout("tweetSpout", new TwitterSpout(), 10);
		builder.setSpout("tweetSpout", new KafkaSpout(kafkaSpoutConfig), 10);
		builder.setBolt("tweetVal", new ExtractBolt(), 5).shuffleGrouping("tweetSpout");
		builder.setBolt("tweetClassify", new ClassifyBolt(), 2).shuffleGrouping("tweetVal");
		builder.setBolt("cassandra", new CassandraBolt(), 4).shuffleGrouping("tweetClassify");
		builder.setBolt("nodejs", new NodeBolt(), 2).shuffleGrouping("tweetClassify");

		builder.setBolt("parse", new ParseBolt(), 3).shuffleGrouping("tweetClassify");
		builder.setBolt("count", new WordBolt(), 2).fieldsGrouping("parse", new Fields("classification"));
		builder.setBolt("nodeWord", new WordNodeBolt(), 2).fieldsGrouping("count", new Fields("classification"));

		// Create new config
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 120);

		if (args != null && args.length > 0) {
	      try{
	      	conf.setNumWorkers(10);
	      	StormSubmitter.submitTopology(args[0], conf, builder.createTopology());	
	      }
	      catch(Exception ex){
	      	ex.printStackTrace();

	      }
	      
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