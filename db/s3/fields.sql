--
--
--

-- drop the table if it exists
DROP TABLE IF EXISTS fields;

-- and create the new one
CREATE TABLE fields (
   id         serial PRIMARY KEY,
   namespace  VARCHAR( 32 ) NOT NULL DEFAULT '' ,
   oid        VARCHAR( 64 ) NOT NULL DEFAULT '',
   name       VARCHAR( 32 ) NOT NULL DEFAULT '',
   value      TEXT NOT NULL DEFAULT '',

   created_at timestamp DEFAULT NOW(),
   updated_at timestamp DEFAULT NOW()
);

-- create the namespace/oid key index
CREATE INDEX fields_key_idx ON fields(namespace, oid);

-- create the distinct index
CREATE UNIQUE INDEX fields_distinct_idx ON fields(namespace, oid, name);

-- auto vacuum parameters
-- see: https://aws.amazon.com/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/
ALTER TABLE fields SET (autovacuum_vacuum_scale_factor = 0.2);  -- 20%
ALTER TABLE fields SET (autovacuum_vacuum_threshold = 1000);
ALTER TABLE fields SET (autovacuum_analyze_scale_factor = 0.1); -- 10%
ALTER TABLE fields SET (autovacuum_analyze_threshold = 1000);

--
-- end of file
--
