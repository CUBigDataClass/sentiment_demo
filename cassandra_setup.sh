CREATE KEYSPACE twitter WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE twitter;

CREATE TABLE tweet (
  tweet_id int,
  tweet text,
  PRIMARY KEY (tweet_id));
