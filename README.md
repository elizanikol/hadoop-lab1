# hadoop-lab1

## Task
1. Бизнес-логика:
Программа, которая агрегирует сырые метрики в выбранный диапазон:
Входной формат: metricId, timestamp (millis), value (справочник обозначений: metricId - metricName)
Выходной формат: metricId, timestamp, scale, value

2. Output Format:
SequenceFile

3. Дополнительные требования:
Использование Custom Type

## Результаты прогона тестов

## HDFS

## Сборка и запуск

Требования к ПО:
```bash
Java 8
Maven
Hadoop 2.8.1
```

В директории lab1 выполнить команду:
```bash
mvn maven
```

При условии успешной сборки и прогона юнит-тестов в директории lab1/target появится jar-файл lab1-1.0-SNAPSHOT-jar-with-dependencies.jar, необходимый для запуска hadoop job в псевдораспределенном кластере.
Запустить сервисы hadoop кластера:
```bash
sbin/start-dfs.sh
sbin/start-yarn.sh
```

Проверить, запущены ли все сервисы, можно командой:
```bash
jps
```

Подготовить файловую систему:
если уже существуют директории input, output их лучше удалить командой: 
```bash
hdfs dfs -rm -r input output
```
во избежание конфликтов при запуске программы.

В программе предусмотрен скрипт генерации сырых метрик ```generateInputData.sh```, для их последующего агрегирования (с помощью функций подсчета суммарного, среднего, максимального или минимального значения) в заданный временной диапазон.

Возможные дополнительные параметры (название генерируемого файла, количество записей, число возможных метрик, временной диапазон для агрегации значений, максимальная величина метрики) скрипта ```generateInputData.sh``` и их значения по умолчанию:
```bash
[root@centos-7 lab1]# ./generateInputData.sh -h
Metrics generator.
 
Syntax: ./generateInputData.sh [-h] [-f FILE] [-n ENTRIES] [-m METRICS] [-s SCALE] [-v MAX_VALUE]
Run without parameters is equivalent to:
./generateInputData.sh -f test -n 100 -m 3 -s 1m -v 100
 
Optional arguments:
-h     help menu;
-f     output file;
-n     number of entries;
-m     number of metricIds;
-s     scale by which you would like to group values in the following format:
          <number><unit>, where unit can be:
          s - for seconds,
          m - for minutes,
          h - for hours,
          d - for days;
-v     max value.
```
Поместить директорию input со сгенерированным тестовым файлом в распределенную файловую систему можно с помощью команды:
```bash
hdfs dfs -put input input
```
Пример запуска программы с параметрами (исходная, выходная директории, файл соответствия ID метрик их названиям, временной интервал и функция агрегации):
```bash
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output config.csv 10s avg
```
Просмотр результатов работы программы возможен из консоли с помощью команды:
```bash
hdfs dfs -text output/p*
```


