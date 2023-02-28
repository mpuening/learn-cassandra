package io.github.learncassandra;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;
import com.github.nosan.embedded.cassandra.cql.CqlScript;

public class EmbeddedCassandraManager {

	/**
	 * Pass in JAVA_HOME to use for Cassandra with this env name
	 */
	public static String JAVA_HOME_ENV_NAME = "JAVA_HOME_8_X86";

	/**
	 * Use this version of Cassandra
	 */
	public static String CASSANDRA_VERSION = "4.0.8";

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

	/**
	 * Apache Cassandra does not run with Java 17, use Java 8. Relies on
	 * JAVA_HOME_8_X86 environment variable being set, for example:
	 * 
	 * export JAVA_HOME_8_X86=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
	 */
	private static void buildServer() {
		String JAVA_HOME = System.getenv(JAVA_HOME_ENV_NAME);
		CASSANDRA = new CassandraBuilder()
				.version(CASSANDRA_VERSION)
				.addEnvironmentVariable("JAVA_HOME", JAVA_HOME)
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
