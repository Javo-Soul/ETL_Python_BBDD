DROP TABLE IF EXISTS public.data_test;

CREATE TABLE IF NOT EXISTS public.data_test
(
    "ID" bigint NOT NULL,
    "Year" bigint,
    "Month" bigint,
    "DayofMonth" bigint,
    "DayOfWeek" bigint,
    "Carrier" text COLLATE pg_catalog."default",
    "OriginAirportID" bigint,
    "OriginAirportName" text COLLATE pg_catalog."default",
    "OriginCity" text COLLATE pg_catalog."default",
    "OriginState" text COLLATE pg_catalog."default",
    "DestAirportID" bigint,
    "DestAirportName" text COLLATE pg_catalog."default",
    "DestCity" text COLLATE pg_catalog."default",
    "DestState" text COLLATE pg_catalog."default",
    "CRSDepTime" bigint,
    "DepDelay" bigint,
    "DepDel15" boolean,
    "CRSArrTime" bigint,
    "ArrDelay" bigint,
    "ArrDel15" boolean,
    "Cancelled" boolean,
    fecha_ts timestamp without time zone,
    CONSTRAINT data_test_pkey PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.data_test
    OWNER to postgres;