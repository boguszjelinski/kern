-- GRANT "kabina" TO pg_read_server_files;
-- 
-- CAB
DROP TABLE cab CASCADE;
CREATE TABLE cab (
    id bigint NOT NULL,
    location integer NOT NULL,
    name character varying(255),
    status integer NOT NULL,
    sits integer NOT NULL
);
ALTER TABLE cab OWNER TO kabina;
ALTER TABLE ONLY cab ADD CONSTRAINT cab_pkey PRIMARY KEY (id);
INSERT INTO cab (id, location, status, sits) SELECT *, 0,2,10 FROM generate_series(0, 10000);

-- CUSTOMER
DROP TABLE customer CASCADE;
CREATE TABLE customer (id bigint NOT NULL);
ALTER TABLE customer OWNER TO kabina;
ALTER TABLE ONLY customer ADD CONSTRAINT customer_pkey PRIMARY KEY (id);
INSERT INTO customer (id) SELECT * FROM generate_series(0, 100000);

-- LEG
DROP TABLE leg CASCADE;
CREATE TABLE leg (
    id bigint NOT NULL,
    completed timestamp without time zone,
    distance integer NOT NULL,
    from_stand integer NOT NULL,
    place integer NOT NULL,
    started timestamp without time zone,
    status integer NOT NULL,
    reserve integer NOT NULL,
    passengers integer NOT NULL,
    to_stand integer NOT NULL,
    route_id bigint NOT NULL
);
ALTER TABLE leg OWNER TO kabina;
ALTER TABLE ONLY leg ADD CONSTRAINT leg_pkey PRIMARY KEY (id);
ALTER TABLE ONLY leg
    ADD CONSTRAINT fk_leg_route_id FOREIGN KEY (route_id) REFERENCES route(id);

-- ROUTE
DROP TABLE route CASCADE;
CREATE TABLE route (
    id bigint NOT NULL,
    status integer NOT NULL,
    cab_id bigint NOT NULL
);
ALTER TABLE route OWNER TO kabina;
ALTER TABLE ONLY route ADD CONSTRAINT route_pkey PRIMARY KEY (id);
ALTER TABLE ONLY route
    ADD CONSTRAINT fk_route_cab_id FOREIGN KEY (cab_id) REFERENCES cab(id);

-- STAT
DROP TABLE stat CASCADE;
CREATE TABLE stat (
    name character varying(255) NOT NULL,
    int_val integer NOT NULL
);
ALTER TABLE stat OWNER TO kabina;
ALTER TABLE ONLY stat ADD CONSTRAINT stat_pkey PRIMARY KEY (name);
INSERT INTO stat (name, int_val) VALUES
    ('AvgExtenderTime', 0),
    ('AvgPoolTime', 0),
    ('AvgPool3Time', 0),
    ('AvgPool4Time', 0),
    ('AvgLcmTime', 0),
    ('AvgSolverTime', 0),
    ('AvgShedulerTime', 0),
    ('MaxExtenderTime', 0),
    ('MaxPoolTime', 0),
    ('MaxPool3Time', 0),
    ('MaxPool4Time', 0),
    ('MaxLcmTime', 0),
    ('MaxSolverTime', 0),
    ('MaxShedulerTime', 0),
    ('AvgDemandSize', 0),
    ('AvgPoolDemandSize', 0),  
    ('AvgSolverDemandSize', 0),
    ('MaxDemandSize', 0), 
    ('MaxPoolDemandSize', 0),
    ('MaxSolverDemandSize', 0),
    ('AvgOrderAssignTime', 0),
    ('AvgOrderPickupTime', 0),
    ('AvgOrderCompleteTime', 0),
    ('TotalLcmUsed', 0),
    ('TotalPickupDistance', 0);

-- STOP
DROP TABLE stop CASCADE;
CREATE TABLE stop (
    id bigint NOT NULL,
    bearing integer,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    name character varying(255),
    no character varying(255),
    type character varying(255)
);
ALTER TABLE stop OWNER TO kabina;
ALTER TABLE ONLY stop ADD CONSTRAINT stop_pkey PRIMARY KEY (id);
--COPY stop(id, no, name, latitude, longitude, bearing) FROM 'stops-Budapest-import.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';

-- ORDER
DROP TABLE taxi_order CASCADE;
CREATE TABLE taxi_order (
    id bigint NOT NULL,
    at_time timestamp without time zone,
    completed timestamp without time zone,
    distance integer NOT NULL,
    eta integer,
    from_stand integer NOT NULL,
    in_pool boolean,
    max_loss integer NOT NULL,
    max_wait integer NOT NULL,
    received timestamp without time zone,
    shared boolean NOT NULL,
    started timestamp without time zone,
    status integer,
    to_stand integer NOT NULL,
    cab_id bigint,
    customer_id bigint,
    leg_id bigint,
    route_id bigint
);
ALTER TABLE taxi_order OWNER TO kabina;
ALTER TABLE ONLY taxi_order ADD CONSTRAINT taxi_order_pkey PRIMARY KEY (id);
ALTER TABLE taxi_order ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME taxi_order_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);
SELECT pg_catalog.setval('taxi_order_id_seq', 1, true);
ALTER TABLE ONLY taxi_order
    ADD CONSTRAINT fk_taxi_order_cab_id FOREIGN KEY (cab_id) REFERENCES cab(id);
ALTER TABLE ONLY taxi_order
    ADD CONSTRAINT fk_taxi_order_customer_id FOREIGN KEY (customer_id) REFERENCES customer(id);
ALTER TABLE ONLY taxi_order
    ADD CONSTRAINT fk_taxi_order_leg_id FOREIGN KEY (leg_id) REFERENCES leg(id);
ALTER TABLE ONLY taxi_order
    ADD CONSTRAINT fk_taxi_order_route_id FOREIGN KEY (route_id) REFERENCES route(id);