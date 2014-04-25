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
import java.util.List;
import org.json.JSONObject;
import org.json.JSONArray;
import java.math.BigDecimal;

import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.LatLng;
import com.google.code.geocoder.model.GeocoderGeometry;

public class ExtractBolt extends BaseRichBolt {
	// Extracts basic information from the tweet object
	OutputCollector _collector;
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      //geocoder = new Geocoder();
    }

    @Override
    public void execute(Tuple tuple) {
    	JSONObject tweetJSON = new JSONObject(tuple.getString(0));
    	String tweet_id = tweetJSON.getString("id_str").trim();
    	String text = tweetJSON.getString("text").trim();
    	String language = tweetJSON.getString("lang");
      String coordinates = new String();
      String userLocation = tweetJSON.getJSONObject("user").getString("location");

      // Find the latitude and longitude
      if(!(tweetJSON.isNull("coordinates"))){
        coordinates = tweetJSON.getJSONObject("coordinates").getJSONArray("coordinates").toString();
      }

      else if(!(tweetJSON.isNull("geo"))){
        coordinates = tweetJSON.getJSONObject("geo").getJSONArray("coordinates").toString();
      }
    	else if(!(userLocation.isEmpty())){
         final Geocoder geocoder = new Geocoder();
          GeocoderRequest geocoderRequest = new GeocoderRequestBuilder().setAddress(userLocation).setLanguage("en").getGeocoderRequest();
          GeocodeResponse geocoderResponse = geocoder.geocode(geocoderRequest);
          List<GeocoderResult> listResponse = geocoderResponse.getResults();
          GeocoderResult data = listResponse.get(0);
          GeocoderGeometry geometry = data.getGeometry();
          LatLng geoCoord = geometry.getLocation();
          BigDecimal latitude = geoCoord.getLat();
          BigDecimal longitude = geoCoord.getLng();

          coordinates = "[" + latitude.toString() + "," + longitude.toString() + "]";
      }
   
 
      _collector.emit(tuple, new Values(tweet_id, text, coordinates));
      _collector.ack(tuple);  

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("tweetID", "tweet", "coordinates"));
    }
}