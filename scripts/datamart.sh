DIR="cleaned_data/cleaned_data"

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
