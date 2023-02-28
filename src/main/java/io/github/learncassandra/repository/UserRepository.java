package io.github.learncassandra.repository;

import java.util.UUID;

import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.stereotype.Repository;

import io.github.learncassandra.model.User;

@Repository
public interface UserRepository extends ReactiveCassandraRepository<User, UUID> {

}
