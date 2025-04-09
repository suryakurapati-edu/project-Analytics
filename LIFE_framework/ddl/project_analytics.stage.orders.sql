CREATE TABLE project_analytics.stage.orders (
    order_sk        UUID PRIMARY KEY,
    order_id        INT UNIQUE,
    customer_id     INT,
    product_id      VARCHAR,
    order_date      DATE,
    quantity        INT,
    total_amount    DECIMAL(10,2),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
