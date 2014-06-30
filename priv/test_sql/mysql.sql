DROP TABLE IF EXISTS boss_db_test_models;
CREATE TABLE boss_db_test_models (
    id                  INTEGER AUTO_INCREMENT PRIMARY KEY,
    some_text           TEXT,
    some_time           DATETIME,
    some_boolean        BOOLEAN,
    some_integer        INTEGER,
    some_float          DOUBLE, -- float too imprecise for mysql, fails tests
    boss_db_test_parent_model_id INTEGER
);
DROP TABLE IF EXISTS boss_db_test_parent_models;
CREATE TABLE boss_db_test_parent_models (
    id                  INTEGER AUTO_INCREMENT PRIMARY KEY,
    some_text           TEXT
);
DROP TABLE IF EXISTS counters;
CREATE TABLE counters (
    name                VARCHAR(255) PRIMARY KEY,
    value               INTEGER DEFAULT 0
);
CREATE FULLTEXT INDEX boss_db_test_models_text on boss_db_test_models(some_text);
