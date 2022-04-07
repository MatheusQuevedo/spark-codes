from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
from datetime import datetime

#function to calculate number of seconds from number of days
minutes = 60
seconds = 60
days = lambda i: i * minutes * seconds * 24
rolling_average_days = 180

data2 = (
  [
    (1,datetime(2022,1,1,0,0,0),1),
    (1,datetime(2021,12,1,0,0,0),2),
    (1,datetime(2021,9,1,0,0,0),3),
    (2,datetime(2022,1,1,0,0,0),4),
    (2,datetime(2021,1,1,0,0,0),5),
    (3,datetime(2022,1,1,0,0,0),6),
    (3,datetime(2021,7,1,0,0,0),7),
    (3,datetime(2021,2,1,0,0,0),8),
    (4,datetime(2022,1,1,0,0,0),9),
  ]
)
  
schema = (
  StructType(
    [
      StructField("id",IntegerType(),True),
      StructField("created_at",TimestampType(),True),
      StructField("transaction_id",IntegerType(),True)      
    ]
  )
)

window_spec = Window.partitionBy('id').orderBy(col('created_at_long')).rangeBetween(-days(rolling_average_days), -1)
  
df = (
  spark.createDataFrame(data2,schema)
    .withColumn('created_at_long', col('created_at').cast('long'))
    .withColumn('t', max('created_at_long').over(window_spec))
    .withColumn('t2', col('t').cast('timestamp'))
)

display(df.orderBy('id','created_at'))