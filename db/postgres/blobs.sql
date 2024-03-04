--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS blobs;

-- and create the new one
CREATE TABLE blobs (
   id         serial PRIMARY KEY,
   bid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   vtag       VARCHAR( 32 ) NOT NULL DEFAULT '',
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   mimetype   VARCHAR( 32 ) NOT NULL DEFAULT '',
   payload    TEXT,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the namespace/oid index
CREATE INDEX blobs_key_idx ON blobs(namespace, oid);

-- create the distinct index
CREATE UNIQUE INDEX blobs_distinct_idx ON blobs(namespace, oid, name);

-- add some dummy data for testing
INSERT INTO blobs(bid,vtag,namespace,oid,name,mimetype,payload) values('bid:cnisrvmdfnu421j85gkg','vtag:cnit9uudfnu55bafteig','libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'metadata.secret.hidden', 'application/json', '{"name":"value"}');
INSERT INTO blobs(bid,vtag,namespace,oid,name,mimetype,payload) values('bid:cnisrvmdfnu421j85gkg','vtag:cnit9uudfnu55bafteig','libraopen','oid:cnfivf6dfnu1a2a5l3fg', 'filename1.txt', 'text/plain', 'bla bla bla');

--
-- end of file
--
