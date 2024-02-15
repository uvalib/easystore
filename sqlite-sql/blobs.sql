--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS blobs;

-- and create the new one
CREATE TABLE blobs (
   id         INTEGER PRIMARY KEY,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   mimetype   VARCHAR( 32 ) NOT NULL DEFAULT '',
--   payload    BLOB,

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the oid index
CREATE INDEX blobs_oid_idx ON blobs(oid);

-- create the distinct index
CREATE UNIQUE INDEX blobs_distinct_idx ON blobs(oid, name);

-- add some dummy data for testing
INSERT INTO blobs(oid,name,mimetype) values('1234567890', 'filename1.txt', 'text/plain');

--
-- end of file
--