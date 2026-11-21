-- Runs once, on first start of an empty data volume, as the superuser, in the default
-- database (`aml`, set by POSTGRES_DB). The aml schema itself is contracts/postgres_schema.sql,
-- mounted as 10_aml_schema.sql so it runs next; 20_roles.sql then gives the two NOLOGIN roles
-- that schema creates a login and a password from the environment.
--
-- Local dev only: mlflow, airflow and the Iceberg catalog connect as the superuser. The API never
-- does (CLAUDE.md rule 11): it connects as aml_api.
CREATE DATABASE mlflow;
CREATE DATABASE airflow;
CREATE DATABASE iceberg_catalog;
