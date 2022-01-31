#!/bin/bash

echo "listing_ID,lat,lng" >> ./data/booking_crawled_result.csv
while read line; do
  id=$(echo $line | cut -d',' -f1)
  url=$(echo $line | cut -d',' -f2 | cut -d'?' -f1)
  data=$(wget -qO- $url)

  latitude=$(echo $data | sed -n "s|^.*b_map_center_latitude\(.*\)$|\1|p" | cut -d'=' -f2 | cut -d';' -f1 | xargs)
  longitude=$(echo $data | sed -n "s|^.*b_map_center_longitude\(.*\)$|\1|p" | cut -d'=' -f2 | cut -d';' -f1 | xargs)
  echo "${id},${latitude},${longitude}" >> ./data/booking_crawled_result.csv
done < data/BOOKING/part-00000-f96353e2-37ad-410f-ae3b-f7b4aa6fa70b-c000.csv