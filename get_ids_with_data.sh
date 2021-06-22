#!/bin/bash
cut -d "," -f2 activity_summary.csv | uniq | tail -n+2 
