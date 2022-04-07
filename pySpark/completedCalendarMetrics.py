from pyspark.sql.functions import *

W = Window.partitionBy(col('id')).orderBy(col('date')).rowsBetween(-5, 0)

dfOriginal = (df)

completedDates = (
    df.groupBy("id")
  .agg(
    date_trunc("mm", max(to_date("current_date", "dd/MM/yyyy"))).alias("max_date"), ## dia máximo
    date_trunc("mm", min(to_date("transaction_date", "dd/MM/yyyy"))).alias("min_date"))
  .select(
    "id",
    expr("sequence(min_date, max_date, interval 1 month)").alias("date"))
  .withColumn("date", explode("date")))

dfFull = (completedDates.alias('b')
          .join(dfOriginal.alias('a'), 
                ['id', 'date'], 
                'left')
            .select('b.*', col('a.date').alias('max_date'), 'a.total_value', 'a.amount_times'))

dfFinal = (dfFull
           .withColumn('dif_months', months_between(col('date'), when(col('total_value').isNull(), max(col('max_date')).over(W)).otherwise(max(col('date')).over(W))))
           .withColumn('qtd_diff_messes', size(collect_set(col('max_date')).over(W)))
           .withColumn('gasto_medio', coalesce(sum(col('total_value')).over(W) / col('qtd_diff_messes'), lit(0)))
           .withColumn('frequencia_media', coalesce(sum(col('amount_times')).over(W) / col('qtd_diff_messes'), lit(0)))
           .withColumn('year_month', date_format(col('date'), 'yyyy/MM')))