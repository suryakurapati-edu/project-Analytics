CREATE TABLE project_analytics.stage.amazon_prime (
    amazon_prime_sk UUID PRIMARY KEY,
    show_id         VARCHAR(255) UNIQUE NOT NULL,
    type            VARCHAR(255),
    title           VARCHAR(255),
    director        VARCHAR(255),
    cast            VARCHAR(255),
    country         VARCHAR(255),
    date_added      TIMESTAMP,
    release_year    INT,
    duration_min    INT,
    duration_season INT,
    rating          VARCHAR(255),
    categories      VARCHAR(1500),
    description     VARCHAR(1500),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);