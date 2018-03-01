suf:
	yarn jar SuffixGen.jar SuffixGen  /input_bio  /output
suftera:
	yarn jar SuffixSort.jar testHdp.SuffixSort -Dmapred.reduce.tasks=64 /input_tera /output_tera
rmr:
	hadoop fs -rm -r /output_tera

compile:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-configuration-1.6.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-lang-2.6.jar:/usr/local/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar:/usr/local/hadoop/share/hadoop/common/lib/spymemcached-2.12.0.jar:./ ./testHdp/*.java
	mv testHdp/*.class output_jar/testHdp/
	rm SuffixSort.jar
	jar -cvf SuffixSort.jar -C output_jar/ .
