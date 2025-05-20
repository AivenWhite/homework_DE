"""Данные
Таблица books:

book_id: ID книги
title: Название книги
author_id: ID автора
genre: Жанр книги
price: Цена книги
publish_date: Дата публикации (в формате YYYY-MM-DD)
Таблица authors:

author_id: ID автора
name: Имя автора
birth_date: Дата рождения автора (в формате YYYY-MM-DD)
country: Страна автора
Выполните следующие пункты по порядку.

1.Чтение данных:

Загрузите данные из CSV файлов в DataFrame. CSV файл books можно скачать по ссылке - https://disk.yandex.ru/d/7ObST0hRb5E4qA , а authors по ссылке - https://disk.yandex.ru/d/jAQHt61PtyxVvg
2. Обработка данных:

Преобразуйте столбцы publish_date и birth_date в формат даты.
3. Объединение данных:

Объедините таблицы books и authors по author_id.
4. Анализ данных:

Найдите топ-5 авторов, книги которых принесли наибольшую выручку.
Найдите количество книг в каждом жанре.
Подсчитайте среднюю цену книг по каждому автору.
Найдите книги, опубликованные после 2000 года, и отсортируйте их по цене.
5. Результаты:

Выведите результаты анализа в виде таблиц."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

books = spark.read.csv("books.csv", header=True, inferSchema=True)
books.printSchema()

authors = spark.read.csv("authors.csv", header=True, inferSchema=True)
authors.printSchema()

data = books.join(authors, "author_id")
data.createTempView("data")
data.show()

#Найдите топ-5 авторов, книги которых принесли наибольшую выручку.

top5_authors = spark.sql("""
    select author_id, name, sum(price) as total_revenue
    from data
    group by author_id, name
    order by total_revenue desc
    limit 5""")
top5_authors.show()

#Найдите количество книг в каждом жанре.
count_books = spark.sql("""
    select genre, count(title) as book_count
    from data
    group by genre""")
count_books.show()

#Подсчитайте среднюю цену книг по каждому автору.
average_price = spark.sql("""
    select author_id, name, avg(price) as average_price
    from data
    group by author_id, name""")
average_price.show()

#Найдите книги, опубликованные после 2000 года, и отсортируйте их по цене.
books_after_2000 = spark.sql("""
    select * 
    from data
    where publish_date > '2000-01-01'
    order by price""")
books_after_2000.show()