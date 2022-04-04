#!/bin/bash
#########################################################################
# Help menu
#########################################################################
usage()
{
  echo "Metrics generator."
  echo
  echo "Syntax: ./generateInputData.sh [-h] [-f FILE] [-n ENTRIES] [-m METRICS] [-s SCALE] [-v MAX_VALUE]"
  echo "Run without parameters is equivalent to:"
  echo "./generateInputData.sh -f test -n 100 -m 3 -s 1m -v 100"
  echo
  echo "Optional arguments:"
  echo "-h     help menu;"
  echo "-f     output file;"
  echo "-n     number of entries;"
  echo "-m     number of metricIds;"
  echo "-s     scale by which you would like to group values in the following format:"
  echo "          <number><unit>, where unit can be:"
  echo "          s - for seconds,"
  echo "          m - for minutes,"
  echo "          h - for hours,"
  echo "          d - for days;"
  echo "-v     max value."
}

#########################################################################
# Script arguments processing
#########################################################################
FILE_NAME="test"
ENTRIES=500
METRICS=4
SCALE=$((60 * 1000)) # 1m
MAX_VALUE=1000

function isDigit {
    re='^[0-9]+$'
    if ! [[ $2 =~ $re ]]
    then
        echo "Error: value of variable $1 is not a number" >&2
        exit 1
    elif [[ $2 -eq 0 ]]
    then
        echo "Error: Zero value of variable $1 is not permitted." >&2
        exit 1
    fi
}

while getopts ":f:n:m:s:v:h" option; do
    case $option in
        h)
           usage
           exit 0
           ;;
        f)
           FILE_NAME=${OPTARG}
           ;;
        n)
           ENTRIES=${OPTARG}
           isDigit ENTRIES $ENTRIES
           ;;
        m)
           METRICS=${OPTARG}
           isDigit METRICS $METRICS;;
        s)
           SCALE=${OPTARG}
           unit=${SCALE: -1}
           SCALE=${SCALE:0:-1}
           multiplier=0
           case $unit in
              s)
                  multiplier=1000;;
              m)
                  multiplier=$((60 * 1000));;
              h)
                  multiplier=$((60 * 60 * 1000));;
              d)
                  multiplier=$((24 * 60 * 60 * 1000));;
              *)
                  echo "Error: Invalid scale unit." >&2
                  exit 1
                  ;;
           esac
           isDigit SCALE $SCALE
           SCALE=$(($SCALE * multiplier))
           ;;
        v)
           MAX_VALUE=${OPTARG}
           isDigit MAX_VALUE $MAX_VALUE
           ;;
       \?)
           echo "Error: Invalid option." >&2
           echo
           usage
           exit 1
           ;;
       *)
           echo "Error: Empty argument value." >&2
           echo
           usage
           exit 1
           ;;
    esac
done

#########################################################################
# Script functionality
#########################################################################
# This script generates input data in the following format:
# metricId, timestamp (millis), value

rm -rf input
mkdir input

timestamp=$(($(date +%s%N)/1000000)) # in ms

for i in $(seq 1 $ENTRIES)
    do
        # generate random METRIC_ID from 1 to METRICS:
        metric_id=$[$RANDOM % $METRICS + 1]
        step=$[$RANDOM % ($SCALE / 4) + ($SCALE / 4)]
        # increment current timestamp by step calculated using the chosen scale (for better distribution):
        timestamp=$(($timestamp + $step))
        # generate value (greater values correspond to greater METRIC_IDs):
        max_value=$(($metric_id * $MAX_VALUE))
        min_value=$((($metric_id - 1) * $MAX_VALUE))
        value=$[$RANDOM % ($max_value - $min_value - 1) + $min_value]
        RESULT="$metric_id, $timestamp, $value"
        echo $RESULT >> input/$FILE_NAME
    done
