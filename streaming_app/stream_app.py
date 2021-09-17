"""
pyspark postgresql streaming main code
"""
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType
from pyspark.sql.functions import from_json, current_timestamp

import db_utils


class StreamApp:
    """
    pyspark streaming application
    """
    def __init__(self, spark):
        self.spark = spark
        self.schema = StructType([
                        StructField('sensor_id', IntegerType()),
                        StructField('event_time', StringType()),
                        StructField('humidity', IntegerType()),
                        StructField('temperature', IntegerType()),
                        StructField('pressure', IntegerType())
                    ])


    def _read_stream(self):
        """
        method to read the data from a kafka topic
        """
        input_df = self.spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("subscribe", "agro") \
                    .option("startingOffsets", "latest") \
                    .load()

        return input_df


    def _process_stream(self):
        """
        extract the records from the input df (pass json schema)
        select the required columns and add some additional timestamps
        """
        input_df = self._read_stream()

        clean_df = input_df \
                    .select(from_json(input_df.value.cast('string'),
                        self.schema).alias('value'))
        explode_df = clean_df.select('value.*')

        enrich_df = explode_df \
                    .withColumn('event_time', explode_df.event_time.cast(TimestampType())) \
                    .withColumn('process_time', current_timestamp())

        return enrich_df


    def _write_stream(self):
        """
        write the streaming df to downstream postgres sink
        """
        enrich_df = self._process_stream()
        df_writer = enrich_df \
                     .writeStream \
                     .queryName("Agro Data Writer") \
                     .foreachBatch(db_utils.foreach_batch_function) \
                     .option("checkpointLocation", "chk-point-dir") \
                     .trigger(processingTime="1 minute") \
                     .start()

        df_writer.awaitTermination()
