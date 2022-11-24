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


-- stage
CREATE TABLE IF NOT EXISTS public.air_pollution_stage (
	"country_name" varchar(256),
    "year" int,
	"air_pollution_index" boolean,
	PRIMARY KEY ("country_id","year")
);


CREATE TABLE  IF NOT EXISTS public.country_stage (
	"coutry_id" varchar(256),
	"country_name" varchar(256)
    PRIMARY KEY ("country_id")
);

