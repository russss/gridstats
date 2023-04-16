CREATE USER gridstats WITH PASSWORD '';
CREATE USER gridstats_read WITH PASSWORD 'gridstats_read';

CREATE DATABASE gridstats OWNER gridstats;
\c gridstats gridstats

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS tablefunc;

CREATE TABLE fuel_type (
    id SERIAL NOT NULL PRIMARY KEY,
    ref TEXT NOT NULL UNIQUE,
    name TEXT,
    interconnector BOOLEAN NOT NULL DEFAULT FALSE,
    country TEXT
);

CREATE TABLE participant (
    ref TEXT NOT NULL PRIMARY KEY,
    name TEXT,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE operator (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TYPE prod_cons AS ENUM ('producer', 'consumer');

INSERT INTO operator (id, name) VALUES 
    (1, 'Electricity North West Limited'),
    (2, 'Northern Powergrid'),
    (3, 'Scottish and Southern Energy'),
    (4, 'ScottishPower Energy Networks'),
    (5, 'UK Power Networks'),
    (6, 'National Grid Electricity Distribution')
;

CREATE TABLE region (
    id INTEGER PRIMARY KEY,
    operator INTEGER NOT NULL REFERENCES operator(id),
    gsp_group TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL
);

INSERT INTO region (id, operator, gsp_group, name) VALUES
    (10, 5, '_A', 'East England'),
    (11, 6, '_B', 'East Midlands'),
    (12, 5, '_C', 'London'),
    (13, 4, '_D', 'North Wales, Merseyside and Cheshire'),
    (14, 6, '_E', 'West Midlands'),
    (15, 2, '_F', 'North East England'),
    (16, 1, '_G', 'North West England'),
    (17, 3, '_P', 'North Scotland'),
    (18, 4, '_N', 'South Scotland'),
    (19, 5, '_J', 'South East England'),
    (20, 3, '_H', 'Southern England'),
    (21, 6, '_K', 'South Wales'),
    (22, 6, '_L', 'South West England'),
    (23, 2, '_M', 'Yorkshire')
;

CREATE TABLE bm_unit_type (
    type TEXT NOT NULL PRIMARY KEY,
    description TEXT
);

INSERT INTO bm_unit_type (type, description) VALUES
    ('T', 'Directly connected'),
    ('E', 'Embedded'),
    ('I', 'Interconnector'),
    ('G', 'Supplier'),
    ('S', 'Supplier (additional)'),
    ('C', 'Supplier CfD'),
    ('V', 'Secondary'),
    ('M', 'Miscellaneous')
    ON CONFLICT(type) DO NOTHING;


CREATE TABLE bm_unit (
    id SERIAL NOT NULL PRIMARY KEY,
    ng_ref TEXT NOT NULL UNIQUE,
    elexon_ref TEXT,
    fuel INTEGER REFERENCES fuel_type(id),
    party_name TEXT,
    type TEXT REFERENCES bm_unit_type(type),
    fpn BOOLEAN,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_seen TIMESTAMPTZ DEFAULT now(),
    unit_name TEXT,
    region INTEGER REFERENCES region(id),
    participant TEXT REFERENCES participant(ref),
    prod_cons prod_cons,
);

CREATE INDEX bm_unit_elexon_ref ON bm_unit(elexon_ref);

CREATE TABLE system_demand (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    demand INTEGER NOT NULL
);

SELECT create_hypertable('system_demand','time');

CREATE TABLE initial_demand_outturn (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    settlement_date DATE NOT NULL,
    settlement_period INTEGER NOT NULL,
    demand_outturn INTEGER,
    transmission_demand_outturn INTEGER
);

SELECT create_hypertable('initial_demand_outturn','time');

CREATE TABLE generation_by_fuel_type_hh (
    time TIMESTAMPTZ NOT NULL,
    settlement_period INTEGER NOT NULL,
    fuel_type INTEGER REFERENCES fuel_type(id),
    generation INTEGER
);

SELECT create_hypertable('generation_by_fuel_type_hh','time');
CREATE UNIQUE INDEX generation_by_fuel_type_hh_time_fuel_type ON generation_by_fuel_type_hh(time, fuel_type);

CREATE TABLE generation_by_fuel_type_inst (
    time TIMESTAMPTZ NOT NULL,
    fuel_type INTEGER REFERENCES fuel_type(id),
    generation INTEGER
);

SELECT create_hypertable('generation_by_fuel_type_inst','time');
CREATE UNIQUE INDEX generation_by_fuel_type_inst_time_fuel_type ON generation_by_fuel_type_inst(time, fuel_type);

CREATE TABLE demand_forecast (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    settlement_period INTEGER NOT NULL,
    transmission_demand INTEGER,
    national_demand INTEGER
);

SELECT create_hypertable('demand_forecast','time');

CREATE TABLE pv_live (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    pv_generation REAL
);
SELECT create_hypertable('pv_live','time');

CREATE TABLE frequency (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    frequency real NOT NULL
);
SELECT create_hypertable('frequency','time');

CREATE TABLE embedded_generation (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    solar_generation INTEGER,
    solar_capacity INTEGER,
    wind_generation INTEGER,
    wind_capacity INTEGER
);
SELECT create_hypertable('embedded_generation','time');

CREATE TABLE embedded_generation_forecast (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    solar_generation INTEGER,
    solar_capacity INTEGER,
    wind_generation INTEGER,
    wind_capacity INTEGER
);
SELECT create_hypertable('embedded_generation_forecast','time');

CREATE VIEW embedded_generation_combined AS
    SELECT time, solar_generation, solar_capacity, wind_generation, wind_capacity
        FROM embedded_generation
    UNION ALL
    SELECT time, solar_generation, solar_capacity, wind_generation, wind_capacity
        FROM embedded_generation_forecast
        WHERE time > (SELECT max(time) FROM embedded_generation);

CREATE TABLE carbon_intensity_national (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    intensity INTEGER NOT NULL
);
SELECT create_hypertable('carbon_intensity_national','time');

CREATE TABLE carbon_intensity_national_forecast (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    intensity INTEGER NOT NULL
);
SELECT create_hypertable('carbon_intensity_national_forecast','time');

CREATE TABLE system_warning (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    warning TEXT NOT NULL
);
SELECT create_hypertable('system_warning','time');

CREATE TABLE stable_export_limit (
    time TIMESTAMPTZ NOT NULL,
    unit TEXT NOT NULL,
    export_limit REAL,
    PRIMARY KEY (time, unit)
);
SELECT create_hypertable('stable_export_limit','time');

CREATE TABLE maximum_export_limit (
    time TIMESTAMPTZ NOT NULL,
    unit TEXT NOT NULL,
    export_limit REAL,
    PRIMARY KEY (time, unit)
);
SELECT create_hypertable('maximum_export_limit','time');

CREATE TABLE maximum_import_limit (
    time TIMESTAMPTZ NOT NULL,
    unit TEXT NOT NULL,
    import_limit REAL,
    PRIMARY KEY (time, unit)
);
SELECT create_hypertable('maximum_import_limit','time');

CREATE TABLE physical_notification (
    time TIMESTAMPTZ NOT NULL,
    unit TEXT NOT NULL,
    level REAL,
    PRIMARY KEY (time, unit)
);
SELECT create_hypertable('physical_notification','time');

CREATE TABLE lolp_dm (
    time TIMESTAMPTZ NOT NULL PRIMARY KEY,
    loss_of_load_probability REAL,
    derated_margin REAL
);
SELECT create_hypertable('lolp_dm','time');

CREATE TABLE wikidata_plants (
    wd_id TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE wd_bm_unit (
    wd_id TEXT NOT NULL REFERENCES wikidata_plants(wd_id),
    bm_unit INTEGER NOT NULL REFERENCES bm_unit(id),
    PRIMARY KEY (bm_unit)
);

GRANT SELECT ON ALL TABLES in schema public TO gridstats_read ;
