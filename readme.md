# ДЗ 4

Выполнил: Иванов Георгий Ярославович \
Группа: БПИ223

После выполнения предыдущих домашних заданий у нас должна 
уже быть среда с развернутым hadoop, yarn и hive.

Сначала на tmpl-jn ставим venv и pip

``` 
sudo apt install python3-venv
sudo apt install python3-pip
```

После этого скачиваем Spark 

``` 
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
```

После этого прописываем этим команды 
``` 
export HADOOP_CONF_DIR="/home/hadoop/hadoop-3.4.0/etc/hadoop"
export HIVE_HOME="/home/hadoop/apache-hive-4.0.1-bin"
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HIVE_AUX_JARS_PATH=$HIVE_HOME/lib/*
export PATH=$PATH:$HIVE_HOME/bin
``` 

Прописываем IP Spark - адрес tmpl-jn
```
export SPARK_LOCAL_IP=192.168.1.18
export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.0/etc/hadoop:/home/hadoop/hadoop-3.4.0/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/common/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"
export PYTHON_PATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):PYTHONPATH
export PATH=$SPARK_HOME/bin:$PATH
```

После этого переходим в директорию spark-3.5.3-bin-hadoop3/

``` 
cd spark-3.5.3-bin-hadoop3/
export SPARK_HOME=`pwd`
pwd
export PYTHON_PATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):PYTHONPATH
export PATH=$SPARK_HOME/bin:$PATH
cd ..
```

Выходим в обратную директорию и активируем python среду 
``` 
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install ipython
```

Скачиваем датасет 

``` 
mkdir input 
cd input 
wget https://github.com/prikokes/dp_hw4/blob/main/CompleteDataset.csv
cd ..
```

Запускаем ipython 

``` 
ipython 
```

Импортируем следующие библиотеки: 
``` 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from onetl.connection import SparkHDFS
from onetl.connection import Hive
from onetl.file import FileDFReader
from onetl.file.format import CSV
from onetl.db import DBWriter
```

Запускаем SparkSession 
``` 
spark = SparkSession.builder \
    .master("yarn") \
    .appName("spark-with-yarn") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.hive.metastore.uris", "thrift://tmpl-jn:5432") \
    .enableHiveSupport() \
    .getOrCreate()
```

Подключаемся, читаем данные из файла, делаем трансформации: 
``` 
hdfs = SparkHDFS(host="tmpl-nn", port=9000, spark=spark, cluster="test")

hdfs.check()
# Out[10]: SparkHDFS(cluster='test', host='tmpl-nn', ipc_port=9000)

reader = FileDFReader(connection=hdfs, format=CSV(delimiter=',', headers=True), source_path='/dir')

df = reader.run(["for_spark.csv"])

df.count()

df_transformed = df.withColumn("Age", F.col("Age").cast("integer")) \
                  .withColumn("Overall", F.col("Overall").cast("integer")) \
                  .withColumn("Potential", F.col("Potential").cast("integer")) \
                  .withColumn("Value", F.regexp_replace("Value", "[€KM]", "").cast("double")) \
                  .withColumn("Wage", F.regexp_replace("Wage", "[€KM]", "").cast("double"))

df_transformed = df_transformed.withColumn("reg_year", F.lit(2023))

df_transformed = df_transformed.withColumn(
    "rating_category",
    F.when(F.col("Overall") >= 90, "World Class")
     .when(F.col("Overall") >= 85, "Elite")
     .when(F.col("Overall") >= 80, "Great")
     .when(F.col("Overall") >= 75, "Good")
     .otherwise("Average")
)

df_transformed = df_transformed.withColumn(
    "age_group",
    F.when(F.col("Age") < 23, "Young")
     .when(F.col("Age") < 30, "Prime")
     .otherwise("Veteran")
)

club_stats = df_transformed.groupBy("Club").agg(
    F.count("Name").alias("player_count"),
    F.avg("Overall").alias("avg_rating"),
    F.avg("Age").alias("avg_age"),
    F.avg("Value").alias("avg_value"),
    F.avg("Wage").alias("avg_wage")
)

nation_stats = df_transformed.groupBy("Nationality").agg(
    F.count("Name").alias("player_count"),
    F.avg("Overall").alias("avg_rating"),
    F.avg("Age").alias("avg_age")
)

writer.run(df_transformed)

club_writer = DBWriter(
    connection=hive, 
    table="test.club_stats", 
    options={"if_exists": "replace_entire_table"}
)
club_writer.run(club_stats)

nation_writer = DBWriter(
    connection=hive, 
    table="test.nation_stats", 
    options={"if_exists": "replace_entire_table"}
)
nation_writer.run(nation_stats)

```

Проверяем данные читая из hive.
``` 
print("Проверка основной таблицы:")
result = spark.sql("SELECT rating_category, COUNT(*) as count FROM test.regs GROUP BY rating_category ORDER BY count DESC")
result.show()

print("Проверка агрегации по клубам:")
result = spark.sql("SELECT Club, player_count, avg_rating FROM test.club_stats ORDER BY avg_rating DESC LIMIT 10")
result.show()

print("Проверка агрегации по странам:")
result = spark.sql("SELECT Nationality, player_count, avg_rating FROM test.nation_stats ORDER BY player_count DESC LIMIT 10")
result.show()

# Проверка чтения через партиции
print("Проверка партиционирования:")
result = spark.sql("SELECT reg_year, COUNT(*) FROM test.regs GROUP BY reg_year")
result.show()

```