### 任务1：PySpark数据处理
    步骤1：使用Python链接Spark环境
    步骤2：创建dateframe数据
    步骤3：用spark执行以下逻辑：找到数据行数、列数
    步骤4：用spark筛选class为1的样本
    步骤5：用spark筛选language >90 或 math> 90的样本


```python
import pandas as pd
import numpy as np

# 同时显示多个结果
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = 'all'

pd.set_option('display.max_columns', None) # 显示完整的列 
pd.set_option('display.max_rows', None) # 显示完整的行 
pd.set_option('display.expand_frame_repr', False) # 设置不折叠数据 
pd.set_option('display.max_colwidth', 10000) # 单列字数宽度，以字符个数计算
```


```python
import pandas as pd
from pyspark.sql import SparkSession

# spark = SparkSession \
#     .builder \
#     .appName('pyspark1') \
#     .getOrCreate()

spark = SparkSession \
    .builder \
    .appName('pyspark2') \
    .getOrCreate()

df = spark.createDataFrame([('001','1',100,87,67,83,98), ('002','2',87,81,90,83,83), ('003','3',86,91,83,89,63),
                            ('004','2',65,87,94,73,88), ('005','1',76,62,89,81,98), ('006','3',84,82,85,73,99),
                            ('007','3',56,76,63,72,87), ('008','1',55,62,46,78,71), ('009','2',63,72,87,98,64)],                           
                             ['number','class','language','math','english','physic','chemical'])

df.show()
```

    /usr/local/lib/python3.6/site-packages/pyspark/context.py:238: FutureWarning: Python 3.6 support is deprecated in Spark 3.2.
      FutureWarning


    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   002|    2|      87|  81|     90|    83|      83|
    |   003|    3|      86|  91|     83|    89|      63|
    |   004|    2|      65|  87|     94|    73|      88|
    |   005|    1|      76|  62|     89|    81|      98|
    |   006|    3|      84|  82|     85|    73|      99|
    |   007|    3|      56|  76|     63|    72|      87|
    |   008|    1|      55|  62|     46|    78|      71|
    |   009|    2|      63|  72|     87|    98|      64|
    +------+-----+--------+----+-------+------+--------+
    



```python
# 找到数据行数、列数
df.count()

len(df.columns)
```




    9






    7




```python
# 筛选class为1的样本
df.filter(df['class']==1).show()
```

    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   005|    1|      76|  62|     89|    81|      98|
    |   008|    1|      55|  62|     46|    78|      71|
    +------+-----+--------+----+-------+------+--------+
    



```python
# 筛选language >90 或 math> 90的样本
df.filter((df['language']>90) | (df['math']>90)).show()
```

    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   003|    3|      86|  91|     83|    89|      63|
    +------+-----+--------+----+-------+------+--------+
    



```python
df.printSchema()
```

    root
     |-- number: string (nullable = true)
     |-- class: string (nullable = true)
     |-- language: long (nullable = true)
     |-- math: long (nullable = true)
     |-- english: long (nullable = true)
     |-- physic: long (nullable = true)
     |-- chemical: long (nullable = true)
    


df.select('class').show()


```python
df.select(df['class'], df['math'] + 1).show()
```

    +-----+----------+
    |class|(math + 1)|
    +-----+----------+
    |    1|        88|
    |    2|        82|
    |    3|        92|
    |    2|        88|
    |    1|        63|
    |    3|        83|
    |    3|        77|
    |    1|        63|
    |    2|        73|
    +-----+----------+
    



```python
df.filter(df['class']==1).show()
```

    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   005|    1|      76|  62|     89|    81|      98|
    |   008|    1|      55|  62|     46|    78|      71|
    +------+-----+--------+----+-------+------+--------+
    



```python
df.filter((df['language']>90) | (df['math']>90)).show()
```

    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   003|    3|      86|  91|     83|    89|      63|
    +------+-----+--------+----+-------+------+--------+
    



```python
df.groupBy('class').count().show()
```

    +-----+-----+
    |class|count|
    +-----+-----+
    |    1|    3|
    |    2|    3|
    |    3|    3|
    +-----+-----+
    



```python
df.createOrReplaceTempView('class')
```


```python
sqlDF = spark.sql("SELECT * FROM class")
sqlDF.show()
```

    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   002|    2|      87|  81|     90|    83|      83|
    |   003|    3|      86|  91|     83|    89|      63|
    |   004|    2|      65|  87|     94|    73|      88|
    |   005|    1|      76|  62|     89|    81|      98|
    |   006|    3|      84|  82|     85|    73|      99|
    |   007|    3|      56|  76|     63|    72|      87|
    |   008|    1|      55|  62|     46|    78|      71|
    |   009|    2|      63|  72|     87|    98|      64|
    +------+-----+--------+----+-------+------+--------+
    



```python
df.createGlobalTempView("class")
spark.sql("SELECT * FROM global_temp.class").show()
```

    +------+-----+--------+----+-------+------+--------+
    |number|class|language|math|english|physic|chemical|
    +------+-----+--------+----+-------+------+--------+
    |   001|    1|     100|  87|     67|    83|      98|
    |   002|    2|      87|  81|     90|    83|      83|
    |   003|    3|      86|  91|     83|    89|      63|
    |   004|    2|      65|  87|     94|    73|      88|
    |   005|    1|      76|  62|     89|    81|      98|
    |   006|    3|      84|  82|     85|    73|      99|
    |   007|    3|      56|  76|     63|    72|      87|
    |   008|    1|      55|  62|     46|    78|      71|
    |   009|    2|      63|  72|     87|    98|      64|
    +------+-----+--------+----+-------+------+--------+
    



```python

```


```python
from pyspark.sql import Row

sc = spark.sparkContext
```


```python
sc 
```





<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://10.180.24.6:4042">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.2.1</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>pyspark2</code></dd>
    </dl>
</div>



