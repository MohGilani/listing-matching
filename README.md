### Project Structure
* `.bsp`: contains an operational file for sbt.
* `data`: contains crawled data from booking.com and vrbo.com.
  * `BOOKING`: contains a csv file with `listing_ID` and `URL` to booking.com, extracted from the provided sample file.
  * `HOMEAWAY`: contains a csv file with `listing_ID` and `URL` to vrbo.com, extracted from the provided sample file.
  * `booking_crawled_result.csv`: contains `listing_ID`, `lat`, and `lng` which I could extract from the `URL` provided in the sample data.
  * `home_away_crawled_result.csv`: contains `listing_ID`, `bathrooms`, `bedrooms`, `lat`, `lng`, and `capacity` which I could extract from the `URL` provided in the sample data.
  * `sample.csv`: This is the sample data provided for this assignment.
* `src`: Contains the actual solution for this problem implemented in Scala.
* `booking_crawler.sh`: a simple script to extract some information from booking.com website. It consumes the csv file in `./data/BOOKING` and for each `listing_ID` queries the provided `URL`.
* `home_away_crawler.sh`: a simple script to extract some information from vrbo.com website. It consumes the csv file in `./data/HOMEAWAY` and for each `listing_ID` queries the provided `URL`.

### Details of Solution
The solution is structured into two steps:
* Read step: reads and cleans data with the following steps to make it ready for solution.   
  1. Reads in `./data/sample.csv`, `./data/booking_crawled_result.csv`, and `./data/home_away_crawled_result`.
  2. Cleans data using UDFs provided in `com.mhsg.hopper.transformations.UDFs`
  3. Joins sample data with the crawled data to populate some missing fields. 
  4. Adds a geoHash column which is calculated using the lat and lng columns of each row.
* Process step: creates a weighted graph between listing and calculates connected components of the graph as the final solution.
  1. Adds a surrogate key to each row in order to identify each listing uniquely for graph creation. 
  2. Joins the cleaned and enriched sample data to itself in order to build edges of the graph. 
  3. For each row, a relationship between two listings of the sample data, calculates a score based on similarity of the fields. 
  4. Filters the rows with high score and selects the two surrogate keys in it (after self-join each row has two surrogate keys).
  5. The result represents edges in our graph and the data from the first step represents our vertexes. 
  6. Builds a graph using spark graphx lib and calculates connect components. 
  7. Groups vertexes in a connected component based on their component_id to get to the final answer.