package twitter.beer;

public class Tweet{

	private Integer tweetID;
	private String text;
	
	public Tweet(Integer id, String tweetText){
		tweetID = id;
		text = tweetText;
	}

	public String getText(){
		return text;
	}

	public Integer getId(){
		return tweetID;
	}
}