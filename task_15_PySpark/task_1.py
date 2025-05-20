"""Данные о погоде представлены в формате CSV и содержат следующие столбцы:

station_id: ID метеостанции
date: Дата наблюдения (в формате YYYY-MM-DD)
temperature: Средняя температура в градусах Цельсия
precipitation: Количество осадков в миллиметрах
wind_speed: Средняя скорость ветра в метрах в секунду
Выполните следующие пункты по порядку.

1. Чтение данных:

Загрузите данные из CSV файла в DataFrame. Скачать CSV нужно по ссылке - https://disk.yandex.ru/d/7Y7ZQgxUKizQsw
2. Обработка данных:

Преобразуйте столбец date в формат даты.
Заполните пропущенные значения, если такие в csv файле есть (например, используя средние значения по метеостанциям).
3. Анализ данных:

Найдите топ-5 самых жарких дней за все время наблюдений.
Найдите метеостанцию с наибольшим количеством осадков за последний год.
Подсчитайте среднюю температуру по месяцам за все время наблюдений.
4. Результаты:

Выведите результаты анализа в виде таблиц."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, when, mean

spark = SparkSession.builder.appName("Analyze weather").getOrCreate()
df = spark.read.csv('weather_data.csv', header=True)

df_with_date = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Заполнение пропущенных значений средними значениями по метеостанциям
mean_temp = df_with_date.groupBy("station_id").agg(mean("temperature").alias("mean_temp"))
mean_precip = df_with_date.groupBy("station_id").agg(mean("precipitation").alias("mean_precip"))
mean_wind = df_with_date.groupBy("station_id").agg(mean("wind_speed").alias("mean_wind"))

weather_df = df_with_date.join(mean_temp, on="station_id", how="left")
weather_df = df_with_date.join(mean_precip, on="station_id", how="left")
weather_df = df_with_date.join(mean_wind, on="station_id", how="left")

weather_df = weather_df.withColumn("temperature",
                                   when(col("temperature").isNull(), col("mean_temp")).otherwise(col("temperature")))
weather_df = weather_df.withColumn("precipitation",
                                   when(col("precipitation").isNull(), col("mean_precip")).otherwise(col("precipitation")))
weather_df = weather_df.withColumn("wind_speed",
                                   when(col("wind_speed").isNull(), col("mean_wind")).otherwise(col("wind_speed")))


"""Найдите топ-5 самых жарких дней за все время наблюдений.
Найдите метеостанцию с наибольшим количеством осадков за последний год.
Подсчитайте среднюю температуру по месяцам за все время наблюдений."""

weather_df.createTempView("weather")
top5temp = spark.sql("""
    select date, temperature
    from weather
    order by temperature desc
    limit 5""")
top5temp.show()

meteostation = spark.sql("""
    select station_id, sum(precipitation) as max_precipitation
    from weather
    WHERE EXTRACT(YEAR FROM date) = (
        SELECT EXTRACT(YEAR FROM MAX(date))
        FROM weather
        )
    group by station_id
    order by max_precipitation desc
    limit 1""")
meteostation.show()

avg_temp = spark.sql("""
    select extract(month from date) as month, AVG(temperature) as avg_temp
    from weather
    group by month
    order by month""")
avg_temp.show()