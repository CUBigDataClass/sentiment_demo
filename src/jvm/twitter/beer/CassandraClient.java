package twitter.beer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class CassandraClient{

	private Cluster cluster;

	public void connect(String node){
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();

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
}