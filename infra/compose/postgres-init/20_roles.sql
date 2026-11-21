-- Runs after 10_aml_schema.sql, which creates aml_service and aml_api as NOLOGIN (see
-- contracts/postgres_schema.sql). Gives them LOGIN and a password taken from the container
-- environment, so no secret lives in a file. \getenv is psql (PG 15+); the value is quoted by
-- psql's :'name' syntax, never by string formatting. If a variable is unset the ALTER ROLE below
-- is a syntax error and the init fails, which is what we want.
--
-- (This is .sql rather than .sh because Docker Desktop's file sharing makes a mounted script look
-- executable, and the entrypoint then runs it directly as the postgres user and fails.)
\getenv svc_pw AML_SERVICE_DB_PASSWORD
\getenv api_pw AML_API_DB_PASSWORD

ALTER ROLE aml_service LOGIN PASSWORD :'svc_pw';
ALTER ROLE aml_api     LOGIN PASSWORD :'api_pw';
