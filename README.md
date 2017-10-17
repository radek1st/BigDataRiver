# BigDataRiver
This project shows how one could simply implement Lambda nad Kappa architectures for making product recommendations
for an imaginary e-commerce store. It is written in Python and employs Kafka, Spark (in Jupyter notebook), Cassandra and Falcon.
All the components are tied in with Docker and their relationships are captured in `docker-compose`.   

## Presentation
Here are the [slides](YowData-RadekOstrowski.pdf) from my presentation at [Yow! Data Sydney](http://data.yowconference.com.au/profile/?id=radek-ostrowski) and [Sydney Docker Meetup](https://www.meetup.com/Sydney-Docker-User-Group/events/238792843/).

## Setup
For the ease of deployment Docker Compose script is used. It still needs some manual steps, however.

* Docker
  * Edit `docker-compose.yml` file and replace paths in `volumes` to match your environment
  * To start all the services run this command from the main project folder: `docker-compose up`
* Simulation of user clicks/actions
  * In a terminal, go to the `data` folder and start a feeder script POSTing JSON messages to Falcon: `./user-simulator.py`
* Cassandra  
  * In another terminal, connect to Cassandra instance with command like:
  `docker exec -it bigdatariver_cassandra_1 bash`
     * Once inside, initialise Cassandra's keyspace: `cqlsh -f bdr/init.sql`
     * You can also run `cqlsh` and start issuing CQL statements directly against Cassandra
* Spark Notebook
  * In a browser, navigate to `http://localhost:8888/` and choose `Lambda - Stream - Users who bought X also bought`.
  Shift+Enter all the cells or choose from the top menu: Cell->Run All
  * Once Spark Streaming is running and the data feeder is started, you should see the recommendation table become populated in Cassandra
  * Repeat the same for other notebooks if required:
    * `Lambda - Batch- Users who bought X also bought`
    * `Kappa - Users who bought X also bought`
    * `Kappa - Collaborative Filtering`
* Falcon
  * Once every gear is in motion, you can finally get the recommendations. Open a browser (or otherwise issue GET request) 
  to hit Falcon and get recommendations like this: 
    * Lambda: `http://127.0.0.1:8000/bdr?product-lambda=59` should return response like `{"product":59, "recommendedProducts":[29,49,99,19,62]}`
    * Kappa: `http://127.0.0.1:8000/bdr?product-kappa=41` should return response like `{"product":41, "recommendedProducts":[21,5,95,83,37]}`
    * Kappa Collaborative Filtering user customised recommendation: `http://127.0.0.1:8000/bdr?user=2105` with response like 
    `{"user":2105, "recommendedProducts":[77,5,95,83,37]}`


## Troubleshooting
If required to connect to Kafka from local host (outside of Docker), add `kafka` hostname to your `/etc/hosts` like this:
~~~~
more /etc/hosts
127.0.0.1       localhost kafka
~~~~
