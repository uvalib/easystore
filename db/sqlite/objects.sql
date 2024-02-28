--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS objects;

-- and create the new one
CREATE TABLE objects (
   id         INTEGER PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   accessid   VARCHAR( 32 ) NOT NULL DEFAULT '',

   created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create the namespace/oid index
CREATE UNIQUE INDEX objects_key_idx ON objects(namespace, oid);

-- add some dummy data for testing
INSERT INTO objects(namespace, oid, accessid) values('libraopen', 'oid:cnfivf6dfnu1a2a5l3fg', 'aid:cnfj2umdfnu1dp1130u0');

--
-- end of file
--