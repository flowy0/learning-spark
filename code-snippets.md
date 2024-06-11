# Some Code Snippets in Pyspark


## Grouping
```python
grouped_df = df.groupBy("town").agg(count("town")) \
    .withColumnRenamed("count(town)","cnt")  
```    

### adding aggregates and windows
```python
import pyspark.sql.functions as f
from pyspark.sql.window import Window

new_df = grouped_df.withColumn('percent', f.col('cnt')/f.sum('cnt').over(Window.partitionBy()))
new_df.orderBy('percent', ascending=False).show()
```

### adding a calculated percentage column
```python
grouped_df = df.groupBy("town").agg(count("town")) \
    .withColumnRenamed("count(town)","cnt")  \
    .withColumn('percent', f.col('cnt')/f.sum('cnt').over(Window.partitionBy()))
    
grouped_df.orderBy('percent', ascending=False).show(30)
```

sample output:

```
+---------------+-----+--------------------+
|           town|  cnt|             percent|
+---------------+-----+--------------------+
|       SENGKANG|12305| 0.08523711225945886|
```



### Joins


```python
grouped_df_train = trainingData.groupBy("town").agg(count("town")) \
    .withColumnRenamed("count(town)","cnt")  \
    .withColumn('pct_train', (f.col('cnt')/f.sum('cnt').over(Window.partitionBy())*100))
    
grouped_df_test = testData.groupBy("town").agg(count("town")) \
    .withColumnRenamed("count(town)","cnt")  \
    .withColumn('pct_test', (f.col('cnt')/f.sum('cnt').over(Window.partitionBy())*100))
```

```python
joined_df = grouped_df_train.join(grouped_df_test,grouped_df_train.town ==  grouped_df_test.town,"left") \
    .drop(grouped_df_test.town)
 
joined_df.orderBy('pct_train', ascending=False).show(30)
```

sample output:
```
+----+-------------------+---------------+----+-------------------+
| cnt|          pct_train|           town| cnt|           pct_test|
+----+-------------------+---------------+----+-------------------+
|9784|  8.458911511693252|       SENGKANG|2521|  8.784890406662717|
```