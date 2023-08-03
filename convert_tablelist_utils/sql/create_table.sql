USE "ARES"."ETL";

CREATE TABLE IF NOT EXISTS UTILS_CREO_DQ (
    table_name VARCHAR,
    Key_column VARCHAR,
    date_field VARCHAR
);

CREATE TABLE IF NOT EXISTS UTILS_CREO (
    table_name VARCHAR,
    sql VARCHAR,
    has_date_entered VARCHAR,
    key_column VARCHAR
);

CREATE TABLE IF NOT EXISTS UTILS_CM_DQ (
    table_name VARCHAR,
    Key_column VARCHAR,
    date_field VARCHAR
);

CREATE TABLE IF NOT EXISTS UTILS_CM (
    table_name VARCHAR,
    sql VARCHAR,
    has_date_entered VARCHAR,
    key_column VARCHAR
);

CREATE TABLE IF NOT EXISTS UTILS_LD_DQ (
    table_name VARCHAR,
    Key_column VARCHAR,
    date_field VARCHAR
);

CREATE TABLE IF NOT EXISTS UTILS_LD (
    table_name VARCHAR,
    sql VARCHAR,
    has_date_entered VARCHAR,
    key_column VARCHAR
);
