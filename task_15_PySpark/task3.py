from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#Загрузите данные из CSV файлов в Spark DataFrame. Все что необходимо скачать находится по ссылке - https://disk.yandex.ru/d/EJU5clkFkhWklA
actors = spark.read.csv("actors.csv", header=True, inferSchema=True)
actors.printSchema()

movies = spark.read.csv("movies.csv", header=True, inferSchema=True)
movies.printSchema()

movie_actors = spark.read.csv("movie_actors.csv", header=True, inferSchema=True)
movie_actors.printSchema()

#Создайте временные таблицы для данных о фильмах, актерах и связях между ними.
actors.createOrReplaceTempView("actors")
movies.createOrReplaceTempView("movies")
movie_actors.createOrReplaceTempView("movie_actors")

#Найдите топ-5 жанров по количеству фильмов.
top5genre = spark.sql("""
    select genre, count(movie_id) as num_movies
    from movies
    group by genre
    order by num_movies desc
    limit 5""")
top5genre.show()

#Найдите актера с наибольшим количеством фильмов.
top_actor = spark.sql("""
    select a.name, count(ma.movie_id) as count_movies 
    from actors a
    join movie_actors ma on a.actor_id = ma.actor_id 
    group by a.name
    order by count(ma.movie_id) desc
    limit 1""")
top_actor.show()

#Подсчитайте средний бюджет фильмов по жанрам.
avg_budget = spark.sql("""
    select genre, avg(budget) as avg_budget
    from movies
    group by genre""")
avg_budget.show()

#Найдите фильмы, в которых снялось более одного актера из одной страны.
movies_with_many_actors = spark.sql("""
    select m.title, a.country, count(*) as count_actors
    from movies m
    join movie_actors ma on ma.movie_id = m.movie_id
    join actors a on a.actor_id = ma.actor_id
    group by title, country
    having count_actors > 1""")
movies_with_many_actors.show()