## Experimental Single-source RPQ evaluation branch

This branch contains an experimental branch for single source RPQ evaluation under arbitrary path semantics.

A sample configuration file is given under `conf\singlesource\ldbc-sf10-arbitrary.json`


#### Requirements 
- Java 13
- Maven 3

#### Build

`mvn package`

#### Configuration

- dataset: Edge list containing the streaming graph
- report-folder: Directory to store log messages during execution
- executable: absolute path of the fat jar file created following build
- runs: array of individual configurations that will be executed in order
  - query-name: use robotic1 to implement reachability, it is essentially the RPQ `a+` where `a` is an edge lavel
  - window-size: should be greater than the entire timestamp range of the streaming graph
  - source-vertex: vertex ID of the source vertex in the graph
  - labels: use a single label for single-source reachability

#### Execute

Use `manual-runner.py` script to directly run from a configuration file:

```
scripts/manual-runner.py conf/singlesource/ldbc-sf10-arbitrary.json
```

Result aggregator script can be used to parse log files and create a single csv with all results

```
scripts/result-aggregator.py /home/apacaci/sgraffito/results/singlesource agg.csv
```

First parameter is the `report-folder` in the configuration file and second parameter is the absolute path for the aggregate result file
