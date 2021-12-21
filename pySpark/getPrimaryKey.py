##REF: https://www.datarchy.tech/code/20200820_spark_rows_number/

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id, shiftRight, broadcast, sum, count, when, lit
from pyspark.sql import Window

class primaryKey:

    def createDataframe():

        data = [("Matheus", "Kebert", 106),
        ("Vinicius", "Frango", 80),
        ("Rafael", "Muito Frango", 102),
        ("Matheus", "Snatch", 150),
        ("Josue", "Frango", 50)]

        schema = StructType([
        StructField("nome", StringType(), True),
        StructField('sobrenome', StringType(), True),
        StructField('peso', IntegerType(), True)
        ])

        dataframe = spark.createDataFrame(data, schema)
        key = 'peso_id'
        
        return dataframe, key

    def getPrimaryKey(dataframe, key):
  
        cols = dataframe.columns
        cols.insert(0, key)
    
        dfID = (dataframe
            .withColumn('row_id', monotonically_increasing_id())
            .withColumn('partition_id', shiftRight('row_id', 33))
            .withColumn('row_offset', col('row_id').bitwiseAND(2147483647))
            )

        dfGrouped = (dfID
                    .groupBy('partition_id')
                    .agg(count('*').alias('partition_size')))

        AcumulativeSum = Window.orderBy(col('partition_id')).rowsBetween(Window.unboundedPreceding, -1)

        dfPartition = (dfGrouped
                    .withColumn('partition_offset_zero', sum(col('partition_size')).over(AcumulativeSum))
                    .withColumn('partition_offset', 
                                when(col('partition_offset_zero').isNull(), lit(0))
                                .otherwise(col('partition_offset_zero'))))

        dfFinal = (dfID
                    .join(broadcast(dfPartition), "partition_id")
                    .withColumn(key, col('partition_offset')+col('row_offset') + 1)
                .select(cols))

        return dfFinal

dataframe, key = primaryKey.createDataframe()
final = primaryKey.getPrimaryKey(dataframe, key)