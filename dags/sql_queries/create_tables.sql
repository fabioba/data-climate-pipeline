CREATE TABLE  IF NOT EXISTS public.country (
	"country_name" varchar(256),
	"country_id" varchar(256),
    PRIMARY KEY("country_id")
);


CREATE TABLE IF NOT EXISTS public.temperature_country (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar,
	PRIMARY KEY("country_name","date_time")
);

CREATE TABLE IF NOT EXISTS public.temperature_state (
	"date_time" timestamp,
	"avg_temperature" float,
	"state_name" varchar,
	"country_name" varchar,
	PRIMARY KEY("state_name","date_time")
);

CREATE TABLE IF NOT EXISTS public.climate (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar,
	"country_id" varchar,
	"max_temperature_state" float,
	"min_temperature_state" float,
	"distinct_state" int,
	PRIMARY KEY("country_id","date_time")
);


CREATE TABLE IF NOT EXISTS public.temperature_country_stage (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar
);


CREATE TABLE IF NOT EXISTS public.temperature_state_stage (
	"date_time" timestamp,
	"avg_temperature" float,
	"state_name" varchar,
	"country_name" varchar
);


CREATE TABLE  IF NOT EXISTS public.country_stage (
	"country_name" varchar(256),
	"country_id" varchar(256)
);

