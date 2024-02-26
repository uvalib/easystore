--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS metadata;

-- and create the new one
CREATE TABLE metadata (
   id         serial PRIMARY KEY,
   oid        VARCHAR( 32 ) UNIQUE NOT NULL,
   accessid   VARCHAR( 64 ) UNIQUE NOT NULL,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- add some dummy data for testing
INSERT INTO metadata(oid, accessid) values('oid:494af4cda213', 'aid:a90f18cde697');

--
-- end of file
--