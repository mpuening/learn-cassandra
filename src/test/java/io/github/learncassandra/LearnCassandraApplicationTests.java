package io.github.learncassandra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import io.github.learncassandra.model.User;
import io.github.learncassandra.repository.UserRepository;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@SpringBootTest
class LearnCassandraApplicationTests {

	@BeforeAll
	public static void startCassandra() throws Exception {
		EmbeddedCassandraManager.start();
	}

	@AfterAll
	public static void stopCassandra() {
		EmbeddedCassandraManager.stop();
	}

	@Autowired
	protected UserRepository userRepository;

	@Test
	public void contextLoads() {
		Assertions.assertNotNull(userRepository);

		Flux<User> users = userRepository.findAll();
		StepVerifier.create(users)
				.assertNext(this::assertUser)
				.assertNext(this::assertUser)
				// Should be none left
				.expectNextCount(0)
				.verifyComplete();
	}

	private void assertUser(User user) {
		Assertions.assertTrue("Alice".equals(user.getFirstName()) || "Bob".equals(user.getFirstName()));
		Assertions.assertNotNull(user.getId());
		Assertions.assertNotNull(user.getUsername());
		Assertions.assertNotNull(user.getLastName());
		Assertions.assertNotNull(user.getAge());
	}
}
