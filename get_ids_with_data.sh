#!/bin/bash
cut -d "," -f2 activity_summary.csv | tail -n+2 | sort | uniq  
