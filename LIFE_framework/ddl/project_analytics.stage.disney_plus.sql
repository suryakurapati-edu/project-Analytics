CREATE TABLE project_analytics.stage.disney_plus(
    show_id         VARCHAR(10) NOT NULL,
    type            VARCHAR(20) NOT NULL,
    title           VARCHAR(100) NOT NULL,
    director        VARCHAR(50) NOT NULL,
    cast            VARCHAR(100) NOT NULL,
    country         VARCHAR(50) NOT NULL,
    date_added      TIMESTAMP,
    release_year    INT
    duration_min    INT
    duration_season INT
    rating          VARCHAR(20) NOT NULL,
    categories      VARCHAR(50) NOT NULL,
    description     VARCHAR(255) NOT NULL,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);