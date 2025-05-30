CREATE TABLE IF NOT EXISTS public.data_test
(
    "ID" bigint PRIMARY KEY,
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
    fecha_ts text COLLATE pg_catalog."default"
);

ALTER TABLE IF EXISTS public.data_test
    OWNER TO postgres;
