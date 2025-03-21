CREATE TABLE users (
    id int PRIMARY KEY,
    full_name text,
    "from" text
);

CREATE TABLE users2 (
    id int PRIMARY KEY,
    full_name text
);

CREATE TABLE AllDatatypeTransformation (
    varchar_column text PRIMARY KEY,
    tinyint_column tinyint,
    text_column text,
    date_column date,
    smallint_column smallint,
    mediumint_column int,
    int_column int,
    bigint_column bigint,
    float_column float,
    double_column double,
    decimal_column decimal,
    datetime_column timestamp,
    timestamp_column timestamp,
    time_column time,
    year_column text,
    char_column text,
    tinytext_column text,
    mediumtext_column text,
    longtext_column text,
    enum_column text,
    bool_column boolean,
    other_bool_column boolean,
    list_text_column list<text>,
    list_int_column list<int>,
    frozen_list_bigint_column frozen<list<bigint>>,
    set_text_column set<text>,
    set_date_column set<date>,
    frozen_set_bool_column frozen<set<boolean>>,
    map_text_to_int_column map<text, int>,
    map_date_to_text_column map<date, text>,
    frozen_map_int_to_bool_column frozen<map<int, boolean>>,
    map_text_to_list_column map<text, frozen<list<text>>>,
    map_text_to_set_column map<text, frozen<set<text>>>,
    set_of_maps_column set<frozen<map<text, int>>>,
    list_of_sets_column list<frozen<set<text>>>,
    frozen_map_text_to_list_column map<text, frozen<list<text>>>,
    frozen_map_text_to_set_column map<text, frozen<set<text>>>,
    frozen_set_of_maps_column set<frozen<map<text, int>>>,
    frozen_list_of_sets_column list<frozen<set<text>>>,
    varint_column varint
);

CREATE TABLE AllDatatypeColumns (
    varchar_column text PRIMARY KEY,
    tinyint_column tinyint,
    text_column text,
    date_column date,
    smallint_column smallint,
    mediumint_column int,
    int_column int,
    bigint_column bigint,
    float_column float,
    double_column double,
    decimal_column decimal,
    datetime_column timestamp,
    timestamp_column timestamp,
    time_column time,
    year_column text,
    char_column text,
    tinytext_column text,
    mediumtext_column text,
    longtext_column text,
    enum_column text,
    bool_column boolean,
    other_bool_column boolean,
    bytes_column BLOB,
    list_text_column list<text>,
    list_int_column list<int>,
    frozen_list_bigint_column frozen<list<bigint>>,
    set_text_column set<text>,
    set_date_column set<date>,
    frozen_set_bool_column frozen<set<boolean>>,
    map_text_to_int_column map<text, int>,
    map_date_to_text_column map<date, text>,
    frozen_map_int_to_bool_column frozen<map<int, boolean>>,
    map_text_to_list_column map<text, frozen<list<text>>>,
    map_text_to_set_column map<text, frozen<set<text>>>,
    set_of_maps_column set<frozen<map<text, int>>>,
    list_of_sets_column list<frozen<set<text>>>,
    frozen_map_text_to_list_column map<text, frozen<list<text>>>,
    frozen_map_text_to_set_column map<text, frozen<set<text>>>,
    frozen_set_of_maps_column set<frozen<map<text, int>>>,
    frozen_list_of_sets_column list<frozen<set<text>>>,
    varint_column varint,
    inet_column INET
);

CREATE TABLE BoundaryConversionTestTable (
    varchar_column text PRIMARY KEY,
    tinyint_column tinyint,
    smallint_column smallint,
    int_column int,
    bigint_column bigint,
    float_column float,
    double_column double,
    decimal_column decimal,
    bool_column boolean,
    ascii_column ascii,
    text_column text,
    bytes_column blob,
    date_column date,
    time_column time,
    timestamp_column timestamp,
    duration_column duration,
    uuid_column uuid,
    timeuuid_column timeuuid,
    inet_column inet,
    map_bool_column map<boolean, boolean>,
    map_float_column map<float, float>,
    map_double_column map<double, double>,
    map_tinyint_column map<tinyint, tinyint>,
    map_smallint_column map<smallint, smallint>,
    map_int_column map<int, int>,
    map_bigint_column map<bigint, bigint>,
    map_varint_column map<varint, varint>,
    map_decimal_column map<decimal, decimal>,
    map_ascii_column map<ascii, text>,
    map_varchar_column map<text, text>,
    map_blob_column map<blob, blob>,
    map_date_column map<date, date>,
    map_time_column map<time, time>,
    map_timestamp_column map<timestamp, timestamp>,
    map_duration_column map<text, duration>,
    map_uuid_column map<uuid, uuid>,
    map_timeuuid_column map<timeuuid, timeuuid>,
    map_inet_column map<inet, inet>
);