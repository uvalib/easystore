--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS fields;

-- and create the new one
CREATE TABLE fields (
   id         INTEGER PRIMARY KEY,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   value      VARCHAR( 32 ) NOT NULL DEFAULT '',

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the oid index
CREATE INDEX fields_oid_idx ON fields(oid);

-- create the distinct index
CREATE UNIQUE INDEX fields_distinct_idx ON fields(oid, name);

-- add some dummy data for testing
INSERT INTO fields(oid,name,value) values('1234567890', 'thekey', 'thevalue');

--
-- end of file
--