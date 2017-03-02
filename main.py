from betl import *
from datetime import date, timedelta
from pyspark.sql import Row

def __main__(sc, sqlContext, day=None, save=True):
    # Setup some example data
    Person = Row('name', 'age')
    data = zip(['alice', 'ben', 'charles', 'daniel'], range(20,24))
    rdd = sc.parallelize(data).map(lambda x: Person(*x))

    data_frame = convert_rdd(
        sqlContext,
        rdd,
        DataFrameConfig(
            [
                ("name", "name", None, StringType()),
                ("age", "age", None, LongType()),
                ("drinking_age", "age", lambda x: x >= 21, BooleanType())
            ],
            lambda x: True
        )
    )

    return data_frame
