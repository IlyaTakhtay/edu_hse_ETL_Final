-- V1__create_app_user.sql
CREATE USER ${app_username} WITH PASSWORD '${app_password}';
GRANT CONNECT ON DATABASE ${app_database} TO ${app_username};
