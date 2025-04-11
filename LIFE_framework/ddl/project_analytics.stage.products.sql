CREATE TABLE project_analytics.stage.products (
    products_sk     UUID PRIMARY KEY,
    product_id     Varchar UNIQUE,
    name      VARCHAR(255),
    category       VARCHAR(255),
    price           DECIMAL(10,2),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);