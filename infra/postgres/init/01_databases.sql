-- Runs once on first start of the postgres container (as the superuser).
-- Two logical databases on one instance: Airflow metadata and the SmartGrid serving store.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER smartgrid WITH PASSWORD 'smartgrid';
CREATE DATABASE smartgrid OWNER smartgrid;

-- Read-only role for Grafana: dashboards can never modify serving data.
CREATE USER grafana_ro WITH PASSWORD 'grafana_ro';
