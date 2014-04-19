sentiment_demo
==============
To Set Up:
<br/>
Run the TweetProducer according to the directions found in that repository
<br/>
Set up Cassandra:
<code>
cassandra-cli -h hostname -p port -u user -pw password -f cassandra_setup.sh
</code>
Start cassandra: 
<code>
bin/cassandra
</code>

<br/>
Start node.js, can use exampleNodeBolt repository.
<code>
node listen.js
</code>
</br>

To Run:
<br/>
<code>
mvn -f pom.xml clean install  
<br/>
storm jar target/twitter-beer-0.0.1-SNAPSHOT-with-dependencies.jar twitter.beer.TwitterTopology
</code>
