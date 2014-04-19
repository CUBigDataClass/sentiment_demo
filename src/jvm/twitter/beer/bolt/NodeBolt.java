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
import java.net.Socket;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import org.json.JSONObject;
import org.json.JSONArray;

public class NodeBolt extends BaseRichBolt {
	// Extracts basic information from the tweet object
	OutputCollector _collector;
  Socket tcpClient;
  DataOutputStream tweetStream;
  PrintWriter tweetPW;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
     
      try{
         tcpClient = new Socket("localhost", 1337);
         tweetStream = new DataOutputStream(tcpClient.getOutputStream());
         tweetPW = new PrintWriter(tweetStream, true);
         
      } catch(Exception e){
        e.printStackTrace();
      }
     
    }

    @Override
    public void execute(Tuple tuple) {
    	JSONObject tweetJSON = new JSONObject();
    	String tweetId = tuple.getString(0);
      String tweetText = tuple.getString(1);
    	String coordinates = tuple.getString(2);

      tweetJSON.append("tweetID", tweetId);
      tweetJSON.append("text", tweetText);
      tweetJSON.append("coordinates", coordinates);

      tweetPW.println(tweetJSON);
      tweetPW.flush();

  		_collector.emit(tuple, new Values(tweetId));
  		_collector.ack(tuple);
      	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("tweetID"));
    }
}