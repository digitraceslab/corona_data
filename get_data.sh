#!/bin/bash

python3 remove_deleted_subjects.py >> logs
python3 check_new_subjects.py >> logs
python3 acceslink.py >> logs
./get_ids_with_data.sh > ids_with_data 

echo done at $(date) >> logs

#cp *.csv ~/raw_data/

