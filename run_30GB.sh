mvn clean package && hadoop fs -rm -r -f /output_30GB && hadoop jar target/MR_Redis-1.0-SNAPSHOT-jar-with-dependencies.jar sinica.iis.SuffixArrayRun /input_30GB /output_30GB

