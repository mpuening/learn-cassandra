CREATE KEYSPACE IF NOT EXISTS test 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE test.user(

	id			uuid,
	user_name	varchar,
	first_name	varchar,
	last_name	varchar,
	age			int,

	PRIMARY KEY(id)
);

CREATE INDEX user_last_name_idx on test.user(last_name);
