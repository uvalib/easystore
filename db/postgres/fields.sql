--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS fields;

-- and create the new one
CREATE TABLE fields (
   id         serial PRIMARY KEY,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   value      VARCHAR( 32 ) NOT NULL DEFAULT '',

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the oid index
CREATE INDEX fields_oid_idx ON fields(oid);

-- create the distinct index
CREATE UNIQUE INDEX fields_distinct_idx ON fields(oid, name);

-- add some dummy data for testing
INSERT INTO fields(oid,name,value) values('oid:494af4cda213', 'thekey', 'thevalue');

--
-- end of file
--
