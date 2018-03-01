exe:
	hadoop jar /usr/local/hadoop/hadoop-*examples*.jar terasort /user/hduser/input_bio_message /user/hduser/output
suf:
	yarn jar SuffixGen.jar SuffixGen  /input_bio  /output
suftera:
	yarn jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar terasort -Dmapred.reduce.tasks=16 /user/hduser/input_tera /user/hduser/output_tera
rmr:
	hadoop fs -rm -r /output

compile:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-configuration-1.6.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-lang-2.6.jar:/usr/local/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar:/usr/local/hadoop/share/hadoop/common/lib/spymemcached-2.12.0.jar:./ *.java
	 mv *.class output_jar/
	rm SuffixGen.jar
	jar -cvf SuffixGen.jar -C output_jar/ .
