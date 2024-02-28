--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS fields;

-- and create the new one
CREATE TABLE fields (
   id         serial PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   value      VARCHAR( 32 ) NOT NULL DEFAULT '',

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the namespace/oid key index
CREATE INDEX fields_key_idx ON fields(namespace, oid);

-- create the distinct index
CREATE UNIQUE INDEX fields_distinct_idx ON fields(namespace, oid, name);

-- add some dummy data for testing
INSERT INTO fields(namespace,oid,name,value) values('libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'key1', 'value1');
INSERT INTO fields(namespace,oid,name,value) values('libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'key2', 'value2');
INSERT INTO fields(namespace,oid,name,value) values('libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'key3', 'value3');

--
-- end of file
--
