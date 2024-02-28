--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS objects;

-- and create the new one
CREATE TABLE objects (
   id         serial PRIMARY KEY,
   oid        VARCHAR( 32 ) UNIQUE NOT NULL,
   accessid   VARCHAR( 64 ) UNIQUE NOT NULL,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- add some dummy data for testing
INSERT INTO objects(oid, accessid) values('oid:cnfivf6dfnu1a2a5l3fg', 'aid:cnfj2umdfnu1dp1130u0');

--
-- end of file
--