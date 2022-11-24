"""
This module includes the ingestion from S3 bucket to another folder on S3 bucket.

Author: Fabio Barbazza
Date: Nov, 2022
"""
import logging 
import yaml
import os

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
import pyspark.sql.functions as sql_func

def get_config(path_yaml):
    """
    This method is responsible for reading the config file

    Args:
        path_yaml(str): path of the file
    
    Returns:
        config_project(dict)
    """
    try:

        logger.info('read_config: {}'.format(path_yaml))

        # Read YAML file
        with open(path_yaml, 'r') as stream:
            config_project = yaml.safe_load(stream)

        logger.info('config project: {}'.format(config_project))

        return config_project

    except Exception as err:
        logger.exception(err)
        raise err



def create_spark_session():
    """
        This method create an instance of SparkSession

        Returns:
            spark(SparkSession)
    """
    try:

        spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", '') \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", '') \
        .getOrCreate()

        return spark
    
    except Exception as err:
        logger.exception(err)
        raise err


def process_population_data(spark, population_input_path, population_output_path):
    """
    This method is responsible for processing song data.
    It stores songs and artists tables

    Args:
        spark(spark session)
        population_input_path(str): input of the file
        population_output_path(str): output path
    """
    try:

        df_population = spark.read.csv(population_input_path)

        # remove duplicates
        df_population_unique = df_population.dropDuplicates()

        df_population_unique = df_population_unique.withColumnRenamed('Country Name', 'country_name').withColumnRenamed('year', 'year').withColumnRenamed('count', 'population_total')

        logger.info('df_population_unique schema'.format(df_population_unique.printSchema()))
        # create complete path on S3
        population_complete_path = os.path.join(population_output_path,'population.parquet')

        logger.info('population_complete_path: {}'.format(population_complete_path))

        # store partitioned by country (create fake column)
        df_population_unique.withColumn("country_partition", sql_func.col("country_name")).write.mode('overwrite').partitionBy("country_partition").parquet(population_complete_path)


    except Exception as err:
        logger.exception(err)
        raise err




def main():
    """
        This is the entrypoint of the module
    """
    try:
        logger.info('starting')

        config_project = get_config('config/config_project.yaml')

        spark = create_spark_session()

        population_input_path = config_project['DATA_SOURCE']['POPULATION_DATA']['INPUT_PATH']
        population_output_path = config_project['DATA_SOURCE']['POPULATION_DATA']['OUTPUT_PATH']

        process_population_data(spark, population_input_path, population_output_path)    

        logger.info('success')

    except Exception as err:
        logger.exception(err)
        raise err


if __name__ == "__main__":
    main()
