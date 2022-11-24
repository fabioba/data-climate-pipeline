"""
This module contains the SqlQueries which is responsible for running insert statements

Author: Fabio Barbazza
Date: Nov, 2022
"""

class SqlQueries:
    """
    A class used to perform inserting statements

    Attributes:
    - insert_air_pollution(str)
    - country_table_insert(str)
    """
    insert_air_pollution = ("""
        SELECT
            distinct country_name,
            year,
            air_pollution_index
        FROM 
            air_pollution_stage
        LEFT JOIN 
            country
        ON 
            air_pollution_stage.country_id = country.country_id
    """)

    # perform join to remove any duplicates
    country_table_insert = ("""
        SELECT 
            distinct country_id, country_name
        FROM 
            country_stage
        left join 
            country
        on 
            country_stage.country_id = country.country_id
        where
            country.country_id is NULL
    """)

