# Ideas for the future
1. Additional queries (see below)
2. Provide package name
3. Store keystore/truststore in project and use in connecting to DSE
    * Will need to figure out how to extract the keystore/truststore to file system
4. Detect Search and enable additional queries


## Additional Queries
1. Partition keys + regular column with ALLOW FILTERING
2. Partition keys + clustering column(s) + regular column with ALLOW FILTERING
3. Partition keys + clustering column(s) with inequality (lt, lte, gt, gte) with ALLOW FILTERING
4. Partition keys + clustering column(s) with inequality (between) with ALLOW FILTERING
5. Partition keys + clustering columns with multiple inequalities
6. Clustering column(s) without parttion key(s) - equality and inequality
7. Regular column scan with ALLOW FILTERING (Danger, Will Robinson)
8. Search queries
    * Wildcards on strings
    * Inequalities without ALLOW FILTERING
    * Geospatial on geospatial types
9. Aggregates on number columns
    * SUM, MIN, MAX, AVG
    * GROUP BY partition keys
    * GROUP BY partition keys and clustering column(s)
