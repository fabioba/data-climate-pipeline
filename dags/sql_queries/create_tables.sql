CREATE TABLE  IF NOT EXISTS public.country (
	"country_id" varchar(256),
	"country_name" varchar(256),
    PRIMARY KEY("country_id")
);


CREATE TABLE IF NOT EXISTS public.temperature (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar,
	PRIMARY KEY("country_name","date_time")
);

CREATE TABLE IF NOT EXISTS public.climate (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar,
	"country_id" varchar,
	PRIMARY KEY("country_id","date_time")
);


CREATE TABLE IF NOT EXISTS public.temperature_stage (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar
);


CREATE TABLE  IF NOT EXISTS public.country_stage (
	"country_id" varchar(256),
	"country_name" varchar(256)
);

