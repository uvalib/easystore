--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS objects;

-- and create the new one
CREATE TABLE objects (
   id         serial PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL,
   oid        VARCHAR( 32 ) NOT NULL,
   accessid   VARCHAR( 64 ) UNIQUE NOT NULL,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the distinct index
CREATE UNIQUE INDEX objects_distinct_idx ON objects(namespace, oid);

-- add some dummy data for testing
INSERT INTO objects(namespace, oid, accessid) values('libraopen', 'oid:cnfivf6dfnu1a2a5l3fg', 'aid:cnfj2umdfnu1dp1130u0');

--
-- end of file
--
