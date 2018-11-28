# Measure latencies between DSE clusters
provides approximate measure. Very imprecise due to lag between DC (gives a max value but not the real inter-dc lag)

## Building
`mvn clean package`

## Running
`java -Dlatency.precisionInMs=2 -Dlatency.password=cassandra -Dlatency.user=cassandra -Dlatency.ks=replication_test -Dlatency.dc1.contactPoint=35.238.71.176 -Dlatency.dc2.contactPoint=35.192.97.184 -Dlatency.dc1.name=dc1 -Dlatency.dc2.name=dc2 -Dlatency.resetCount=200 -jar /home/quentin/projects/latencies/target/demo-0.1-SNAPSHOT-jar-with-dependencies.jar`
