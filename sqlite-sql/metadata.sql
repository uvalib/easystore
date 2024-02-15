--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS metadata;

-- and create the new one
CREATE TABLE metadata (
   id         INTEGER PRIMARY KEY,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   accessid   VARCHAR( 64 ) NOT NULL DEFAULT '',

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the oid index
CREATE INDEX metadata_oid_idx ON metadata(oid);

-- create the distinct index
CREATE UNIQUE INDEX metadata_distinct_idx ON metadata(oid);

-- add some dummy data for testing
INSERT INTO metadata(oid, accessid) values('1234567890', '13d95c74-ce2d-4316-9c9c-26b6487ca3b3');

--
-- end of file
--