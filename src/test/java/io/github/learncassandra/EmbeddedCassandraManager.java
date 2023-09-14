package io.github.learncassandra;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;
import com.github.nosan.embedded.cassandra.cql.CqlScript;

public class EmbeddedCassandraManager {

	/**
	 * Use this version of Cassandra...
	 *
	 * Can't find this version? Check valid versions here: https://downloads.apache.org/cassandra/
	 *
	 * Downloaded versions are cached here: $HOME/.embedded-cassandra/cassandra
	 *
	 * Versions 4 and below need Java 8, not Java 17
	 */
	public static String CASSANDRA_VERSION = "5.0-alpha1";

	private static Cassandra CASSANDRA = null;

	public static synchronized void start() {
		if (!isRunning()) {
			if (CASSANDRA == null) {
				buildServer();
			}
			CASSANDRA.start();
			loadData();
		}
	}

	public static synchronized void stop() {
		if (isRunning() && CASSANDRA != null) {
			CASSANDRA.stop();
		}
	}

	public static synchronized boolean isRunning() {
		return (CASSANDRA != null) ? CASSANDRA.isRunning() : false;
	}

	public static synchronized Settings getSettings() {
		return (CASSANDRA != null) ? CASSANDRA.getSettings() : null;
	}

	private static void buildServer() {
		CASSANDRA = new CassandraBuilder()
				.version(CASSANDRA_VERSION)
				.registerShutdownHook(true)
				.build();
	}

	private static void loadData() {
	    Settings settings = CASSANDRA.getSettings();
	    try (CqlSession session = CqlSession.builder() 
	            .addContactPoint(new InetSocketAddress(settings.getAddress(), settings.getPort()))
	            .withLocalDatacenter("datacenter1")
	            .build()) {
	        CqlScript.ofClassPath("/schema.cql").forEachStatement(session::execute);
	        CqlScript.ofClassPath("/data.cql").forEachStatement(session::execute);
	    }
	}
}
