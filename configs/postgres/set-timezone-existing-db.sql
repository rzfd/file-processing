-- Script to set timezone for existing databases
-- Run this manually if needed: docker exec -i file-processing-postgres psql -U postgres < configs/postgres/set-timezone-existing-db.sql

-- Set timezone for fileprocessing database
ALTER DATABASE fileprocessing SET timezone = 'Asia/Jakarta';

-- Set timezone for postgres default database
ALTER DATABASE postgres SET timezone = 'Asia/Jakarta';

-- Verify timezone
SELECT name, setting, source FROM pg_settings WHERE name = 'TimeZone';

