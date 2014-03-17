package twitter.beer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class CassandraClient{

	private Cluster cluster;
	private Session session;

	public void connect(String node){
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		session = cluster.connect();

		// TODO: Remove this after testing
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()){
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s;\n", host.getDataCenter(), host.getAddress(), host.getRack());
		}
	}

	public void close(){
		// Shutdown cluster instance when finished
		cluster.shutdown();
	}

	public void createSchema(String keyspace, String schemaClass){
		session.execute("CREATE KEYSPACE " + keyspace + " WITH replication= {'class': '" + schemaClass + "', 'replication_factor': 3};");
	}

	public void createTable(String keyspace, String tableName, String columns){
		session.execute("CREATE TABLE " + keyspace + "." + tableName + "(" + columns ");");
	}

	public void loadData(String insertStatement){
		// Change this to include Prepared and Bound Statements once the database schema has been implemented
		// http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/quick_start/qsSimpleClientBoundStatements_t.html
		session.execute(insertStatement);
	}

	public void querySchema(String selectStatement){
		session.execute(selectStatement);
	}
}