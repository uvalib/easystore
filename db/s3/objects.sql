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
   vtag       VARCHAR( 32 ) UNIQUE NOT NULL,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the namespace/oid index
CREATE UNIQUE INDEX objects_key_idx ON objects(namespace, oid);

--
-- end of file
--
