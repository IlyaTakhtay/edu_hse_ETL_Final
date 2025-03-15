CREATE SCHEMA IF NOT EXISTS ${app_schema};
GRANT USAGE ON SCHEMA ${app_schema} TO ${app_username};
GRANT CREATE ON SCHEMA ${app_schema} TO ${app_username};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${app_schema} TO ${app_username};
ALTER DEFAULT PRIVILEGES IN SCHEMA ${app_schema} GRANT ALL ON TABLES TO ${app_username};
