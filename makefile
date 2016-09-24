exe:
	#hadoop jar ProcessBmp.jar ProcessBmp /user/hduser/input_one_min_nc /user/hduser/output
	#hadoop jar ProcessBmp.jar ProcessBmp /user/hduser/input_one_min /user/hduser/output
	hadoop jar SuffixArrayRun.jar SuffixArrayRun /input_bio /output_bio
rmr:
	hadoop fs -rm -r /output_bio
compile:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-configuration-1.6.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-lang-2.6.jar:/usr/local/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar:/usr/local/hadoop/share/hadoop/common/lib/spymemcached-2.12.0.jar:/usr/local/hadoop/share/hadoop/common/lib/jedis-3.0.0-SNAPSHOT.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-pool2-2.4.2.jar:./ *.java
	mv *.class output_jar/
	rm SuffixArrayRun.jar
	jar -cvf SuffixArrayRun.jar -C output_jar/ .
