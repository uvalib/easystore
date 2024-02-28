--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS blobs;

-- and create the new one
CREATE TABLE blobs (
   id         INTEGER PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   mimetype   VARCHAR( 32 ) NOT NULL DEFAULT '',
   payload    BLOB,

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the namespace/oid index
CREATE INDEX blobs_key_idx ON blobs(namespace, oid);

-- create the distinct index
CREATE UNIQUE INDEX blobs_distinct_idx ON blobs(namespace, oid, name);

-- add some dummy data for testing
INSERT INTO blobs(namespace,oid,name,mimetype,payload) values('libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'metadata.secret.hidden', 'application/json', '{"name":"value"}');
INSERT INTO blobs(namespace,oid,name,mimetype,payload) values('libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'filename1.txt', 'text/plain', 'bla bla bla');

--
-- end of file
--