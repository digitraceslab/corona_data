#!/bin/bash

python3 check_new_subjects.py >> logs
python3 remove_deleted_subjects.py >> logs
python3 acceslink.py >> logs
./get_ids_with_data.sh > ids_with_data 
cat exercise_samples/exercise_samples*.csv > exercise_samples.csv

echo done at $(date) >> logs

