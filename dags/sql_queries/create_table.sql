CREATE TABLE IF NOT EXISTS public.temperature (
	"temperature_id" id INT IDENTITY(0,1),
	"datetime" timestamp,
	"avg_temperature" boolean,
	"country_id" varchar
    PRIMARY KEY("temperature_id")
);

CREATE TABLE IF NOT EXISTS public.time (
	"date_time" timestamp NOT NULL,
	"year" int,
	PRIMARY KEY ("date_time")
);


CREATE TABLE  IF NOT EXISTS public.country (
	"coutry_id" varchar(256),
	"country_name" varchar(256)
    PRIMARY KEY ("country_id")
);


CREATE TABLE IF NOT EXISTS public.air_pollution (
	"country_id" varchar(256),
	"year" int,
	"air_pollution_index" boolean,
	CONSTRAINT "songs_pkey" PRIMARY KEY ("country_id","year")
);

CREATE TABLE  IF NOT EXISTS public.population (
	"country_id" varchar,
	"year" int,
	"total_population" int
    PRIMARY KEY ("country_id","year")
);

CREATE TABLE IF NOT EXISTS public.temperature (
	"temperature_id" id INT IDENTITY(0,1),
	"datetime" timestamp,
	"avg_temperature" boolean,
	"country_id" varchar
    PRIMARY KEY("temperature_id")
);

-- stage
CREATE TABLE IF NOT EXISTS public.air_pollution_stage (
	"country_name" varchar(256),
	"country_id" varchar(256),
    "year" int,
	"air_pollution_index" boolean,
	PRIMARY KEY ("country_id","year")
);

CREATE TABLE  IF NOT EXISTS public.population_stage (
	"country_name" varchar,
	"year" int,
	"total_population" int
    PRIMARY KEY ("country_name","year")
);

CREATE TABLE IF NOT EXISTS public.temperature_stage (
	"date_time" timestamp,
	"avg_temperature" boolean,
	"country_name" varchar
    PRIMARY KEY("date_time","country_name")
);
