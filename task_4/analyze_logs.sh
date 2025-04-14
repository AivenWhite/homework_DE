#!/bin/bash

if [[ ! -f "access.log" ]]; then
    echo "Файл access.log не найден."
    exit 1
fi

(
    echo "Отчёт о логе веб-сервера"
    echo "========================"

    # Общее количество запросов
    echo "Общее количество запросов: $(grep -c '^.*HTTP/1\.1"' access.log)"

    # Количество уникальных IP-адресов
    echo "Количество уникальных адресов: $(awk '{print $1}' access.log | sort | uniq | wc -l)"

    # Количество запросов по методам
    echo -e "\nКоличество запросов по методам:"
    awk '{gsub(/"/, "", $6); method[$6]++} END {for (m in method) print method[m], m}' access.log

    # Самый популярный URL
    echo -e "\nСамый популярный URL:"
    awk '{url[$7]++} END {for (u in url) if (url[u] > max) {max = url[u]; popular = u} print popular, max}' access.log
) > report.txt

echo "Отчёт сохранён в файл report.txt"
