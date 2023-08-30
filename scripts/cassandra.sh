#!/bin/bash

DIR="cleaned_data/cleaned_data"

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 src/create_table.py -d selected_OFF.csv
echo "Sleep for 10 s after transport data to cassandra"
sleep 10

for i in {0..100..2}
do
    if [ -d "$DIR" ]; then
        echo "DATA WAS CLEANED SUCCESSFULLY"
        break
    else
        spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 src/datamart.py -s cleaned_data 
        echo "DATA WAS NOT CLEANED SUCCESSFULLY. PROGRAM WILL TRY ANOTHER TIME"
    fi
done

echo "Sleep for 10 s after using datamart for downloading, cleaning data from cassandra. It also saved cleaned data" 
sleep 10
spark-submit src/model.py -f cleaned_data -s prediction_data
echo "Sleep for 10 s after using k-means algorithm and saving final csv file with predictions. After that final data will be downloaded to cassandra via src/create_table.py program"
sleep 10
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 src/create_table.py -d prediction_data -t prediction -tqi 0



