# DATA-CLIMATE-PIPELINE

## Table of content
- [Business Context](#business_context)
- [Data Sources](#data_sources)
- [System Design](#system_design)
- [System Design - Data Ingestion](#system_design_data_ingestion)
- [System Design - Data Lake](#system_design_data_lake)
- [System Design - Data Transformation/Loading](#system_design_data_transformation)
- [System Design - Data Warehouse](#system_design_data_warehouse)


<a name="business_context"/>

## Business Context
The goal of this project is to analyze the relationship between:
* `air pollution`
* `temperature` 
* `population`

These are some of the questions that it will be possible to answer:
* What is the Country with the highest increase of `air pollution`?
* What is the Country with the highest increase of `temperature`?
* What is the Country with the highest negative correlation between `pollution` and `population`?

Eventually, this is the core question:
* What is the best Country to live in the future?

<a name="data_sources"/>

## Data Sources
* `air pollution`: https://www.kaggle.com/datasets/pavan9065/air-pollution 
* `temperature`: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data?resource=download 
* `population`: https://www.kaggle.com/datasets/imdevskp/world-population-19602018?select=population_total_long.csv 

<a name="system_design"/>

## System Design
![alt](docs/images/data_climate_workflow.drawio.png)

<a name="system_design_data_ingestion"/>

### System Design - Data Ingestion
The goal of this step is to ingest the data from the data sources and break it down into partitions. Those partitions are arbitrary considering the type of input data. For instance, for this specific case it's better broken down each dataset by date. 

<a name="system_design_data_lake"/>

### System Design - Data Lake
Below there's the folder structure of the data lake on S3:
```
│
├── air_pollution/
│     └── air_pollution_date=DD/MM/YYYY
├── temperature/
│     └── temperature_date=DD/MM/YYYY
└── population/
      └── population_date=DD/MM/YYYY
```

<a name="system_design_data_transformation"/>

### System Design - Data Transformation/Loading

<a name="system_design_data_warehouse"/>

### System Design - Data Warehouse

