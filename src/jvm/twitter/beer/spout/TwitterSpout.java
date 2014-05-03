package twitter.beer.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import twitter.beer.Tweet;

// Taken from the Storm Starter for practice purposes
// https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/spout/RandomSentenceSpout.java
public class TwitterSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  BufferedReader _reader;


  @Override
  public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
    try{
       _reader = new BufferedReader(new FileReader("/home/kira/Downloads/beerTweets.txt"));   
    } 
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }
    
   
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    try{
        // Generates a random Integer to emit
      int line_num = _rand.nextInt(1000) + 1;
      Utils.sleep(100);
      String line = _reader.readLine();

      if(line != null){
        _collector.emit(new Values(line));
      }

    } catch(IOException e ){e.printStackTrace();}    

    
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("initialTweet"));
  }

}