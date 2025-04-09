CREATE TABLE project_analytics.stage.customers (
    customer_sk     UUID PRIMARY KEY,
    customer_id     INT UNIQUE,
    first_name      VARCHAR(255) NOT NULL,
    last_name       VARCHAR(255) NOT NULL,
    email           VARCHAR,
    city            VARCHAR,
    state           VARCHAR(2),
    signup_date     DATE,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);