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

--
-- end of file
--