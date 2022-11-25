# DATA-CLIMATE-PIPELINE

## Table of content
- [Business Context](#business_context)
- [Data Sources](#data_sources)
- [System Design](#system_design)
    * [System Design - Data Lake](#system_design_data_lake)
    * [System Design - Data Transformation/Loading](#system_design_data_transformation)
    * [System Design - Data Warehouse](#system_design_data_warehouse)
- [Business Questions for the future](#business_questions_future)

<a name="business_context"/>

## Business Context
The goal of this project is to analyze the relationship between:
* `temperature`
* `country`

These are some of the questions that it will be possible to answer:
* What is the Country with the highest `temperature` in 2003?
```sql
SELECT 
    country_name
FROM 
    climate
where 
    date_trunc('year',date_time) = 2003
qualify row_number() over(partition by date_trunc('year',date_time) order by avg_temperature) = 1

```

* What is the Country with the highest increase of `temperature` over last 10 years?
```sql
with temperature_avg_year as (
    SELECT 
    country_name,
    avg(avg_temperature) as avg_temperature_year
    FROM 
        climate
    group by 1
)
SELECT 
    country_name,
    lag(avg_temperature_year,10) over(partition by country_name order by date_time asc) as avg_temperature_10_years_ago,
    avg_temperature_year - avg_temperature_10_years_ago as change_temperature_in_10_years
FROM 
    temperature_avg_year

```

Eventually, this is the core question:
* What is the best Country to live in the future?

<a name="data_sources"/>

## Data Sources
* `temperature`: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download&select=GlobalLandTemperaturesByCountry.csv
* `country`: https://www.kaggle.com/datasets/programmerrdai/outdoor-air-pollution

<a name="system_design"/>

## System Design
![alt](docs/images/data_climate_workflow.drawio.png)

The first step regards the Data Lake which is developed on S3. So, it's easy to add unstructured files on the system. For our pourpose we have CSV files with a pre-defined structured, so it's even easier to maintain them.
Then, the data source files are handled by Airflow. The workflow developed with that tool aim to clean the data source data and populate the Data Warehouse which sits on AWS - Redshift.
The DAG should be run on a monthly basis. Since the data source is updated at the beginning of each month, this workflow should be run from the day after.

<a name="system_design_data_lake"/>

### System Design - Data Lake
Below there's the folder structure of the data lake on S3:
```
bucket: data-climate
│
├── temperature_country.csv
└── country.csv
```


<a name="system_design_data_transformation"/>

### System Design - Data Transformation/Loading
This steps is responsible for cleaning, transforming and loading data into datawarehouse.
Before running the pipeline, make sure to create connections to AWS from `Airflow Connection` section.

![alt](docs/images/dag.png)



<a name="system_design_data_warehouse"/>

### System Design - Data Warehouse
The image below refers to the data warehouse diagram, where:

```
country (
	"country_name" varchar(256),
	"country_id" varchar(256),
    PRIMARY KEY("country_id")
);


temperature_country (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar,
	PRIMARY KEY("country_name","date_time")
);

temperature_state (
	"date_time" timestamp,
	"avg_temperature" float,
	"state_name" varchar,
	"country_name" varchar,
	PRIMARY KEY("state_name","date_time")
);

climate (
	"date_time" timestamp,
	"avg_temperature" float,
	"country_name" varchar,
	"country_id" varchar,
	"max_temperature_state" float,
	"min_temperature_state" float,
	"distinct_state" int,
	PRIMARY KEY("country_id","date_time")
);

```

![alt](docs/images/er.drawio.png)


<a name="business_questions_future"/>

# Business Questions for the future
* What If the data was increased by 100x?
    * If the data was increased by 100x I could elaborate them using PySpark on a cluster, so each node will operate on a subset of the input dataset
* What If the pipelines were run on a daily basis by 7am?
    * In that case I would create a condition on the query to slice data based on the date_time daily basis
* What If the database needed to be accessed by 100+ people?
    * In that case I would enable auto scaling of the Redshift cluster to be able to scale out and scale in following users requests