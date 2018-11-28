# Measure latencies between DSE clusters
provides approximate measure. Very imprecise due to lag between DC (gives a max value but not the real inter-dc lag)

## Building
`mvn clean package`

## Running
`java -jar target/latencies-0.1-SNAPSHOT-jar-with-dependencies.jar <options>`
