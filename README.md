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
<img width="1361" alt="Screenshot 2022-04-20 at 01 20 39" src="https://user-images.githubusercontent.com/55412039/164111534-9fd08010-bd0c-4ecf-bfc0-50679a196d27.png">

## HDFS
Скриншот входного сгенерированного файла, загруженного в HDFS:
<img width="590" alt="Screenshot 2022-04-20 at 01 12 52" src="https://user-images.githubusercontent.com/55412039/164110644-681916d0-2fbf-4971-8ffd-f909b2b74bce.png">

Скриншот выходного Sequence файла в HDFS:

<img width="586" alt="Screenshot 2022-04-20 at 01 12 27" src="https://user-images.githubusercontent.com/55412039/164110633-0ce24751-84fe-4583-9fd7-254ce34976fa.png">

Результаты работы программы в более читаемом формате:

<img width="446" alt="Screenshot 2022-04-20 at 01 08 16" src="https://user-images.githubusercontent.com/55412039/164110865-8b6145f5-5cd2-4a52-a6b6-869337de5700.png">

## MapReduce job

## Сборка и запуск
<img width="1351" alt="Screenshot 2022-04-20 at 01 00 34" src="https://user-images.githubusercontent.com/55412039/164110740-10405a2d-aa47-4c27-b7ca-3e73eac8ed60.png">
<img width="1344" alt="Screenshot 2022-04-20 at 01 01 04" src="https://user-images.githubusercontent.com/55412039/164110765-a9aacecd-bedb-4259-a454-b715b146c29f.png">

Требования к ПО:
```bash
Java 8
Maven
Hadoop 2.8.1
```

В директории ```lab1``` выполнить команду:
```bash
mvn maven
```

При условии успешной сборки и прогона юнит-тестов в директории ```lab1/target``` появится jar-файл ```lab1-1.0-SNAPSHOT-jar-with-dependencies.jar```, необходимый для запуска hadoop job в псевдораспределенном кластере.

Запустить сервисы hadoop кластера (из директории ```/opt/hadoop*/```):
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

Возможные дополнительные параметры (название генерируемого файла, количество записей, число возможных метрик, временной диапазон для агрегации значений, диапазон значений метрики) скрипта ```generateInputData.sh``` и их значения по умолчанию:
```
[root@centos-7 lab1]# ./generateInputData.sh -h
Metrics generator.
 
Syntax: ./generateInputData.sh [-h] [-f FILE] [-n ENTRIES] [-m METRICS] [-s SCALE] [-r RANGE]
Run without parameters is equivalent to:
./generateInputData.sh -f test -n 500 -m 4 -s 1m -r 1000
 
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
-r     values range.
```
Поместить директорию input со сгенерированным тестовым файлом в распределенную файловую систему можно с помощью команды:
```bash
hdfs dfs -put input input
```
Пример запуска программы с параметрами (исходная, выходная директории, файл соответствия ID метрик их названиям, временной интервал и функция агрегации):
```bash
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output config.csv 15m avg
```
Просмотр результатов работы программы возможен из консоли с помощью команды:
```bash
hdfs dfs -text output/p*
```


