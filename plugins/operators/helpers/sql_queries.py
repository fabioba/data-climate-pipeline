"""
This module contains the SqlQueries which is responsible for running insert statements

Author: Fabio Barbazza
Date: Nov, 2022
"""

class SqlQueries:
    """
    A class used to perform inserting statements

    Attributes:
    - insert_climate(str)
    - insert_temperature(str)
    - country_table_insert(str)
    """
    insert_climate = ("""
        SELECT
            distinct 
            temperature.date_time,
            temperature.avg_temperature,
            temperature.country_name,
            country.country_id
        FROM 
            temperature
        LEFT JOIN 
            country
        ON 
            lower(temperature.country_name) = lower(country.country_name)
        where 
            temperature.date_time = '{execution_date}'::timestamp
    """)

    insert_temperature = ("""
        SELECT
            distinct 
            temperature_stage.date_time,
            temperature_stage.avg_temperature,
            temperature_stage.country_name
        FROM 
            temperature_stage
        LEFT JOIN 
            temperature
        ON 
            temperature_stage.date_time = temperature.date_time
        AND
            temperature_stage.country_name = temperature.country_name
        WHERE
            temperature.avg_temperature is NULL
    """)

    # perform join to remove any duplicates
    insert_country = ("""
        SELECT 
            distinct country_stage.country_id, country_stage.country_name
        FROM 
            country_stage
        left join 
            country
        on 
            country_stage.country_id = country.country_id
        where
            country.country_id is NULL
    """)

