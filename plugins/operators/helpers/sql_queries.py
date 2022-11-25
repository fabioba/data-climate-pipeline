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
    - insert_climate_stage(str)
    - insert_temperature(str)
    - country_table_insert(str)
    """
    insert_climate = ("""
        SELECT
            distinct date_time,
            avg_temperature,
            country_name,
            country_id,
            max_temperature_state,
            min_temperature_state,
            distinct_state
        FROM 
            climate_stage
    """)
 
    insert_climate_stage = ("""
        SELECT
            temperature_country.date_time,
            temperature_country.avg_temperature,
            temperature_country.country_name,
            country.country_id,
            max(temperature_state.avg_temperature) as max_temperature_state,
            min(temperature_state.avg_temperature) as min_temperature_state,
            count(temperature_state.state_name) as distinct_state
        FROM 
            temperature_country
        LEFT JOIN 
            temperature_state
        ON 
            temperature_country.country_name = temperature_state.country_name
        JOIN
            country
        ON
            lower(temperature_country.country_name) = (country.country_name)
        LEFT JOIN 
            climate
        ON
            temperature_country.country_name = climate.country_name
            and temperature_country.date_time = climate.date_time
        WHERE 
            climate.state_name is NULL
        group by 
            1,2,3,4
    """)

    insert_temperature_country = ("""
        SELECT
            distinct 
            temperature_country_stage.date_time,
            temperature_country_stage.avg_temperature,
            temperature_country_stage.country_name
        FROM 
            temperature_country_stage
        LEFT JOIN 
            temperature_country
        ON 
            temperature_country_stage.date_time = temperature_country.date_time
        AND
            temperature_country_stage.country_name = temperature_country.country_name
        WHERE
            temperature_country.avg_temperature is NULL
    """)

    insert_temperature_state = ("""
        SELECT
            distinct 
            temperature_state_stage.date_time::timestamp as date_time,
            temperature_state_stage.avg_temperature::float as avg_temperature,
            temperature_state_stage.state_name,
            temperature_state_stage.country_name
        FROM 
            temperature_state_stage
        LEFT JOIN 
            temperature_state
        ON 
            temperature_state_stage.date_time = temperature_state.date_time
        AND
            temperature_state_stage.country_name = temperature_state.country_name
        WHERE
            temperature_state.avg_temperature is NULL
    """)

    # perform join to remove any duplicates
    insert_country = ("""
        SELECT 
            distinct 
                country_stage.country_name, 
                country_stage.country_id
        FROM 
            country_stage
        left join 
            country
        on 
            country_stage.country_id = country.country_id
        where
            country.country_id is NULL
    """)

