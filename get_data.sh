#!/bin/bash

python3 check_new_subjects.py
python3 remove_deleted_subjects.py
python3 acceslink.py
./get_ids_with_data.sh > ids_with_data

