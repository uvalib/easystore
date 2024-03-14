--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS objects;

-- and create the new one
CREATE TABLE objects (
   id         serial PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL,
   oid        VARCHAR( 64 ) NOT NULL,
   vtag       VARCHAR( 32 ) UNIQUE NOT NULL,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the namespace/oid index
CREATE UNIQUE INDEX objects_key_idx ON objects(namespace, oid);

-- auto vacuum parameters
-- see: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
ALTER TABLE objects SET (autovacuum_vacuum_scale_factor = 0.2);  -- 20%
ALTER TABLE objects SET (autovacuum_vacuum_threshold = 1000);
ALTER TABLE objects SET (autovacuum_analyze_scale_factor = 0.1); -- 10%
ALTER TABLE objects SET (autovacuum_analyze_threshold = 1000);

--
-- end of file
--
