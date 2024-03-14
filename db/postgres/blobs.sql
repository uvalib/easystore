--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS blobs;

-- and create the new one
CREATE TABLE blobs (
   id         serial PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 64 ) NOT NULL DEFAULT '',
   name       VARCHAR( 256 ) NOT NULL DEFAULT '',
   mimetype   VARCHAR( 32 ) NOT NULL DEFAULT '',
   payload    BYTEA,

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the namespace/oid index
CREATE INDEX blobs_key_idx ON blobs(namespace, oid);

-- create the distinct index
CREATE UNIQUE INDEX blobs_distinct_idx ON blobs(namespace, oid, name);

-- auto vacuum parameters
-- see: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
ALTER TABLE blobs SET (autovacuum_vacuum_scale_factor = 0.2);  -- 20%
ALTER TABLE blobs SET (autovacuum_vacuum_threshold = 1000);
ALTER TABLE blobs SET (autovacuum_analyze_scale_factor = 0.1); -- 10%
ALTER TABLE blobs SET (autovacuum_analyze_threshold = 1000);

--
-- end of file
--
