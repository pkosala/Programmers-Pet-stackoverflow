# Create a super user
psql postgres -c " CREATE USER pooja WITH PASSWORD 'XXXX';"

# Create a database and grant permission to the user created above

psql postgres -c "
DROP DATABASE IF EXISTS insight;

CREATE DATABASE insight
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

GRANT ALL ON DATABASE insight TO postgres;

GRANT TEMPORARY, CONNECT ON DATABASE insight TO PUBLIC;

GRANT ALL ON DATABASE insight TO pooja;"

# Run script to create tables, functions and import packages
sudo -u postgres psql -f create_table.sql