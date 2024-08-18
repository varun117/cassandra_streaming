import logging
from datetime import datetime
import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_spark_connection():
    s_conn = None

    try:

        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1," "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1," "org.apache.kafka:kafka-clients:3.6.2") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()


        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info('Spark connection created successfully')
    except Exception as e:
        logging.error(f"Couldn't create connection to spark due to exception {e}")

    return s_conn


def create_cassandra_connection():
    session = None

    try:
        cluster = Cluster(['localhost'])

        session = cluster.connect()
        logging.log(20, 'Cassandra connection created successfully')

    except Exception as e:
        logging.error(f"Couldn't create connection cassandra due to exception {e}")
    
    return session


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    logging.log(20, 'Keyspace created successfully')


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT,
        )
    """)

    logging.log(20, 'Table created successfully')


def insert_data(session, **kwargs):
    logging.log(20, 'Inserting data...')

    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, postcode, email, username, registered_date, phone, picture) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, first_name, last_name, gender, address, postcode, email, username, registered_date, phone, picture))

        logging.log(20, 'Data Inserted for '+first_name+' '+last_name)

    except Exception as e:
        logging.error(f"Couldn't insert data into table due to exception {e}")


def create_kafka_connection(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        logging.log(20, 'Created Kafka Connection Successfully')

    except Exception as e:
        
        logging.error(f"Couldn't create kafka dataframe due to exception {e}")

    return spark_df

def create_selection_df(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value as STRING)").select(from_json(col('value'), schema).alias('data')).select('data.*')
    #logging.log(20, sel)
    print(sel)

    return sel



if __name__ == '__main__':
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1 pyspark-shell'

    logging.getLogger().setLevel(logging.INFO)
    spark_conn = create_spark_connection()
   


    if spark_conn is not None:
        spark_df = create_kafka_connection(spark_conn)
        selection_df = create_selection_df(spark_df)
        cass_session = create_cassandra_connection()

        if cass_session is not None:

            logging.log(20, 'Both connections created successfully')

            create_keyspace(cass_session)
            create_table(cass_session)

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'created_users')
                                .start())

            logging.log(20, 'writeStream Query Created, Proceeding to write indefinitely')

            streaming_query.awaitTermination()
        