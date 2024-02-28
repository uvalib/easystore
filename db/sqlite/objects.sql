--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS objects;

-- and create the new one
CREATE TABLE objects (
   id         INTEGER PRIMARY KEY,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   accessid   VARCHAR( 64 ) NOT NULL DEFAULT '',

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the oid index
CREATE INDEX object_oid_idx ON objects(oid);

-- create the distinct index
CREATE UNIQUE INDEX objects_distinct_idx ON objects(oid);

-- add some dummy data for testing
INSERT INTO objects(oid, accessid) values('oid:cnfivf6dfnu1a2a5l3fg', 'aid:cnfj2umdfnu1dp1130u0');

--
-- end of file
--