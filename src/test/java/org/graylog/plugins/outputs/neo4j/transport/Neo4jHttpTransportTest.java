package org.graylog.plugins.outputs.neo4j.transport;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.graylog.plugins.outputs.neo4j.Neo4jOutput;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.shared.bindings.GuiceInjectorHolder;
import org.graylog2.shared.bindings.RestApiBindings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

public class Neo4jHttpTransportTest {

	private static final String NEO4J_USERNAME = "neo4j";
	private static final String NEO4J_PASSWORD = "password";
	private static final String NEO4J_URL = "http://localhost:7474";

	private final Driver neo4jDriver = GraphDatabase.driver(NEO4J_URL.replace("http", "bolt"), AuthTokens.basic(NEO4J_USERNAME, NEO4J_PASSWORD));
	private Neo4JHttpTransport httpTransport;

	@BeforeEach
	void setUp() throws MessageOutputConfigurationException {

		GuiceInjectorHolder.createInjector(Collections.singletonList(new RestApiBindings()));
		neo4jDriver.session().run("MATCH (n:Test) DELETE n").consume();

		Map<String, Object> configValues = new HashMap<>();
		configValues.put(Neo4jOutput.CK_NEO4J_URL, NEO4J_URL);
		configValues.put(Neo4jOutput.CK_NEO4J_USER, NEO4J_USERNAME);
		configValues.put(Neo4jOutput.CK_NEO4J_PASSWORD, NEO4J_PASSWORD);
		configValues.put(Neo4jOutput.CK_NEO4J_STARTUP_QUERY, "RETURN 1");
		configValues.put(Neo4jOutput.CK_NEO4J_QUERY, "CREATE (n:Test {someKey:$someKeyValue})");

		Configuration configuration = new Configuration(configValues);
		httpTransport = new Neo4JHttpTransport(configuration);
	}

	@Test
	void shouldExecuteBoltQuery() {

		Message message = new Message(ImmutableMap.of("_id", "id value", "someKeyValue", "foo"));

		httpTransport.send(message);

		Record result = neo4jDriver.session().run("MATCH (n:Test) RETURN n").single();
		Assertions.assertEquals("foo", result.get("n").asNode().get("someKey").asString());
	}

	@Test
	void shouldNotCreateDataWhenMissingQueryParameter() {

		Message message = new Message(ImmutableMap.of("_id", "id value"));

		httpTransport.send(message);

		Result result = neo4jDriver.session().run("MATCH (n:Test) RETURN n");
		Assertions.assertFalse(result.hasNext());
	}
}
