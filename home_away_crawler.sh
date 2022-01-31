#!/bin/bash

echo "listing_ID,bathrooms,bedrooms,lat,lng,capacity" >> ./data/home_away_crawled_result.csv
while read line; do
  id=$(echo $line | cut -d',' -f1)
  url=$(echo $line | cut -d',' -f2)
  data=$(curl $url | grep "window.__INITIAL_STATE__ = " | sed 's/window.__INITIAL_STATE__ = //g' | sed 's/.$//')
  bathroomCount=$(echo $data | jq  ".listingReducer.spaces.spacesSummary.bathroomCount")
  bedroomCount=$(echo $data | jq  ".listingReducer.spaces.spacesSummary.bedroomCount")
  capacity=$(echo $data | jq ".listingReducer.sleeps")
  latitude=$(echo $data | jq ".listingReducer.geoCode.latitude")
  longitude=$(echo $data | jq ".listingReducer.geoCode.longitude")

  echo "${id},${bathroomCount},${bedroomCount},${latitude},${longitude},${capacity}" >> ./data/home_away_crawled_result.csv
done < data/HOMEAWAY/part-00000-c510e9e0-a2c5-4bda-ba5a-c5a5335e7a34-c000.csv