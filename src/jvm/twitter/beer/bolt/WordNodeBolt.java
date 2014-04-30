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
import org.json.JSONTokener;

public class WordNodeBolt extends BaseRichBolt {
	// Extracts basic information from the tweet object
	OutputCollector _collector;
  Socket tcpClient;
  DataOutputStream tweetStream;
  PrintWriter tweetPW;
  Integer wineCount;
  Integer beerCount;

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
    	JSONObject wordJSON = new JSONObject();
      String word = tuple.getString(0);
      Integer count = tuple.getInteger(1);
      String type = tuple.getString(2);
      Float percentage;

      if(type.equals("wine")){
        if(wineCount == null){
          wineCount = 1;
        }else{
          wineCount++;
        }

        percentage = (float) count / wineCount;

      }else{
        if(beerCount == null){
          beerCount = 1;
        }else{
          beerCount++;
        }

        percentage = (float) count / beerCount;

      }

      if(percentage > 0.1){
        wordJSON.append("word", word);
        wordJSON.append("percent", percentage);
        wordJSON.append("classifcation", type);

        tweetPW.println(wordJSON);
        tweetPW.flush();
      }

  		_collector.emit(tuple, new Values(word, percentage, type));
  		_collector.ack(tuple);
      	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "percentage", "classification"));
    }
}