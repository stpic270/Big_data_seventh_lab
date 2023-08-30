# Big_data_seventh_lab
  В этой работе реализована витрина данных с использованием pyspark. 
  Сначала table и keyspace создаются в cassandra с помощью cassandra-driver (библиотека python), это реализовано в create_table.py. 
  Далее данные (selected_OFF.csv - https://drive.google.com/file/d/135vjuIHEgdUjuhO6Gybjffn3lT2ZJsZY/view?usp=sharing) загружаются в инициализированную таблицу в cassandra в create_table.py. Затем datamart.py использует данные из таблицы cassandra и очищает их, сохраняя очищенный файл csv. Наконец, model.py берет очищенный файл csv и запускает алгоритм k-means для создания меток и сохранения исходного файла и его новых меток в файл csv. 
  В конце create_table.py создает новую таблицу для окончательного CSV с метками и загружает в нее данные.
  Таким образом, model.py ничего не знает о базе данных Cassandra и просто делает прогнозы с помощью алгоритма k-средних. Очистка данных и их загрузка в базу данных cassandra реализованы в datamart.py и create_table.py соответственно. Вы также можете проверить последовательность действий в scripts/cassandra.sh
## 1. После клонирования репо, cначала используйте эту команду, чтобы скачать проект и запустить его:
docker compose build && docker compose up -d
## 2. Передать ip cassandra в volume проекта:
docker exec big_data_seventh_lab-cassandra-1 bash -c "echo '\n' >> config/cassandra_ip.txt && ip -4 -o address >> config/cassandra_ip.txt"
## 3. Создайте пространство ключей, таблицу и загрузите в нее данные:
docker exec big_data_seventh_lab-model-1 bash -c "spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 src/create_table.py -d selected_OFF.csv"
## 4. Очищает данные и сохраняет их в csv для model.py:
docker exec big_data_seventh_lab-model-1 bash -c "scripts/datamart.sh"
## 5. Делает метки с помощью алгоритма k-средних и сохраняет их в файл csv:
docker exec big_data_seventh_lab-model-1 bash -c “spark-submit src/model.py -f cleaned_data -s prediction_data”
## 6. Наконец, перенесите данные в новую таблицу cassandra из ранее созданного файла csv:
docker exec big_data_seventh_lab-model-1 bash -c “spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 src/create_table.py -d prediction_data -t prediction -tqi 0”

## Также вы можете попробовать выполнить все шаги с 3 по 6, просто запустив:
docker exec big_data_seventh_lab-model-1 bash -c “scripts/cassandra.sh”

В конечном результате вы должны получить вывод, похожий на рис. 1 в терминале (со столбцом prediction).

![Screenshot from 2023-08-30 19-29-45](https://github.com/stpic270/Big_data_seventh_lab/assets/58371161/9ec71f74-313a-4170-8a66-276d3668bad0)

рис. 1

Весь процесс может занять 10-15 минут
