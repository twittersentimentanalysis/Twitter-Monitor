# Twitter-Monitor
Twitter Monitor to get filtered tweets and send it to Kafka server.


## How to run
### Local
1. Clone this project to a local folder and go to root folder

   `git clone https://github.com/twittersentimentanalysis/Twitter-Monitor.git`

2. Build the Spring Boot project with Maven

    `mvn clean install`
    
3. Generate jar file to execute the project

    `mvn package`

4. Run the project

    `mvn exec:java -Dexec.mainClass=TwitterMonitor`
    

