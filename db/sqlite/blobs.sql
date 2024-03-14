--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS blobs;

-- and create the new one
CREATE TABLE blobs (
   id         INTEGER PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 64 ) NOT NULL DEFAULT '',
   name       VARCHAR( 256 ) NOT NULL DEFAULT '',
   mimetype   VARCHAR( 32 ) NOT NULL DEFAULT '',
   payload    BLOB,

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the namespace/oid index
CREATE INDEX blobs_key_idx ON blobs(namespace, oid);

-- create the distinct index
CREATE UNIQUE INDEX blobs_distinct_idx ON blobs(namespace, oid, name);

--
-- end of file
--
