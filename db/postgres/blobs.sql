--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS blobs;

-- and create the new one
CREATE TABLE blobs (
   id         serial PRIMARY KEY,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   mimetype   VARCHAR( 32 ) NOT NULL DEFAULT '',
   payload    TEXT,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the distinct index
CREATE UNIQUE INDEX blobs_distinct_idx ON blobs(oid, name);

-- add some dummy data for testing
INSERT INTO blobs(oid,name,mimetype,payload) values('oid:cnfivf6dfnu1a2a5l3fg', 'metadata.secret.hidden', 'application/json', 'eyJpZCI6MTIzLCJuYW1lIjoidGhlIG5hbWUifQ==');
INSERT INTO blobs(oid,name,mimetype,payload) values('oid:cnfivf6dfnu1a2a5l3fg', 'filename1.txt', 'text/plain', 'eyJpZCI6MTIzLCJuYW1lIjoidGhlIG5hbWUifQ==');

--
-- end of file
--
