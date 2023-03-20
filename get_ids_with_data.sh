#!/bin/bash

# This will find the last date we have received data from a given subject.

# find unique IDs in the data
cut -d "," -f2 ../raw_data/activity_summary.csv | tail -n+2 | sort | uniq > .get_last_date_unique

# For each ID, find latest date
# This will output rows with ID and date
while read id; do
  grep $id ../raw_data/activity_summary.csv | tail -n 1 | cut -d "," -f2,3 | tr ',' ' '
done < .get_last_date_unique
