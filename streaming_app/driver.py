from pyspark.sql import SparkSession

from stream_app import StreamApp


# spark session
# -------------------------------------------------
spark = SparkSession \
        .builder \
        .appName('Streaming Job') \
        .getOrCreate()

# create a streaming app object
# -------------------------------------------------
str_app = StreamApp(spark)

# run the streaming application
# -------------------------------------------------
str_app._write_stream()
