package sinica.iis;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class BioPartitioner extends Partitioner<IntWritable,LongWritable> {
  @Override
  public int getPartition(IntWritable DNA_prefix, LongWritable value, int numReduceTasks) {
  //public int getPartition(LongWritable DNA_prefix, LongWritable value, int numReduceTasks) {

  	boolean USING_KEY = false;
	//int keyCount = 196;
	int keyCount = 19567;
    	//int keyCount = 97835;
	int reducerCount=64;	

	if(USING_KEY){
		return (DNA_prefix.get()-1)*reducerCount/(keyCount);
	}
	else{
		if(DNA_prefix.get() <= 307181224)
	  	  return 63;

		if(DNA_prefix.get() > 307181224 && DNA_prefix.get() <= 311426448)
		  return 62;

		if(DNA_prefix.get() > 311426448 && DNA_prefix.get() <= 317145161)
		  return 61;

		if(DNA_prefix.get() > 317145161 && DNA_prefix.get() <= 327499793)
		  return 60;

		if(DNA_prefix.get() > 327499793 && DNA_prefix.get() <= 335852057)
		  return 59;

		if(DNA_prefix.get() > 335852057 && DNA_prefix.get() <= 341709309)
		  return 58;

		if(DNA_prefix.get() > 341709309 && DNA_prefix.get() <= 358088707)
		  return 57;

		if(DNA_prefix.get() > 358088707 && DNA_prefix.get() <= 364840466)
		  return 56;

		if(DNA_prefix.get() > 364840466 && DNA_prefix.get() <= 384631032)
		  return 55;

		if(DNA_prefix.get() > 384631032 && DNA_prefix.get() <= 402885836)
		  return 54;

		if(DNA_prefix.get() > 402885836 && DNA_prefix.get() <= 409475867)
		  return 53;

		if(DNA_prefix.get() > 409475867 && DNA_prefix.get() <= 419505324)
		  return 52;

		if(DNA_prefix.get() > 419505324 && DNA_prefix.get() <= 432148708)
		  return 51;

		if(DNA_prefix.get() > 432148708 && DNA_prefix.get() <= 439218736)
		  return 50;

		if(DNA_prefix.get() > 439218736 && DNA_prefix.get() <= 457731161)
		  return 49;

		if(DNA_prefix.get() > 457731161 && DNA_prefix.get() <= 468069531)
		  return 48;

		if(DNA_prefix.get() > 468069531 && DNA_prefix.get() <= 476562500)
		  return 47;

		if(DNA_prefix.get() > 476562500 && DNA_prefix.get() <= 484288750)
		  return 46;

		if(DNA_prefix.get() > 484288750 && DNA_prefix.get() <= 549334061)
		  return 45;

		if(DNA_prefix.get() > 549334061 && DNA_prefix.get() <= 555224993)
		  return 44;

		if(DNA_prefix.get() > 555224993 && DNA_prefix.get() <= 563416707)
		  return 43;

		if(DNA_prefix.get() > 563416707 && DNA_prefix.get() <= 571714532)
		  return 42;

		if(DNA_prefix.get() > 571714532 && DNA_prefix.get() <= 579919614)
		  return 41;

		if(DNA_prefix.get() > 579919614 && DNA_prefix.get() <= 585888699)
		  return 40;

		if(DNA_prefix.get() > 585888699 && DNA_prefix.get() <= 605256243)
		  return 39;

		if(DNA_prefix.get() > 605256243 && DNA_prefix.get() <= 628107873)
		  return 38;

		if(DNA_prefix.get() > 628107873 && DNA_prefix.get() <= 653004349)
		  return 37;

		if(DNA_prefix.get() > 653004349 && DNA_prefix.get() <= 696232308)
		  return 36;

		if(DNA_prefix.get() > 696232308 && DNA_prefix.get() <= 707991698)
		  return 35;

		if(DNA_prefix.get() > 707991698 && DNA_prefix.get() <= 717498749)
		  return 34;

		if(DNA_prefix.get() > 717498749 && DNA_prefix.get() <= 725308439)
		  return 33;

		if(DNA_prefix.get() > 725308439 && DNA_prefix.get() <= 732389309)
		  return 32;

		if(DNA_prefix.get() > 732389309 && DNA_prefix.get() <= 799707656)
		  return 31;

		if(DNA_prefix.get() > 799707656 && DNA_prefix.get() <= 813029961)
		  return 30;

		if(DNA_prefix.get() > 813029961 && DNA_prefix.get() <= 823196618)
		  return 29;

		if(DNA_prefix.get() > 823196618 && DNA_prefix.get() <= 843543711)
		  return 28;

		if(DNA_prefix.get() > 843543711 && DNA_prefix.get() <= 854051573)
		  return 27;

		if(DNA_prefix.get() > 854051573 && DNA_prefix.get() <= 875520366)
		  return 26;

		if(DNA_prefix.get() > 875520366 && DNA_prefix.get() <= 895097971)
		  return 25;

		if(DNA_prefix.get() > 895097971 && DNA_prefix.get() <= 910748691)
		  return 24;

		if(DNA_prefix.get() > 910748691 && DNA_prefix.get() <= 925382656)
		  return 23;

		if(DNA_prefix.get() > 925382656 && DNA_prefix.get() <= 946871661)
		  return 22;

		if(DNA_prefix.get() > 946871661 && DNA_prefix.get() <= 960134168)
		  return 21;

		if(DNA_prefix.get() > 960134168 && DNA_prefix.get() <= 966796875)
		  return 20;

		if(DNA_prefix.get() > 966796875 && DNA_prefix.get() <= 976349688)
		  return 19;

		if(DNA_prefix.get() > 976349688 && DNA_prefix.get() <= 1040851496)
		  return 18;

		if(DNA_prefix.get() > 1040851496 && DNA_prefix.get() <= 1049724374)
		  return 17;

		if(DNA_prefix.get() > 1049724374 && DNA_prefix.get() <= 1064030997)
		  return 16;

		if(DNA_prefix.get() > 1064030997 && DNA_prefix.get() <= 1073413984)
		  return 15;

		if(DNA_prefix.get() > 1073413984 && DNA_prefix.get() <= 1090546564)
		  return 14;

		if(DNA_prefix.get() > 1090546564 && DNA_prefix.get() <= 1098416561)
		  return 13;

		if(DNA_prefix.get() > 1098416561 && DNA_prefix.get() <= 1118102699)
		  return 12;

		if(DNA_prefix.get() > 1118102699 && DNA_prefix.get() <= 1135352923)
		  return 11;

		if(DNA_prefix.get() > 1135352923 && DNA_prefix.get() <= 1141321682)
		  return 10;

		if(DNA_prefix.get() > 1141321682 && DNA_prefix.get() <= 1151128918)
		  return 9;

		if(DNA_prefix.get() > 1151128918 && DNA_prefix.get() <= 1160156250)
		  return 8;

		if(DNA_prefix.get() > 1160156250 && DNA_prefix.get() <= 1168552412)
		  return 7;

		if(DNA_prefix.get() > 1168552412 && DNA_prefix.get() <= 1171875000)
		  return 6;

		if(DNA_prefix.get() > 1171875000 && DNA_prefix.get() <= 1190043124)
		  return 5;

		if(DNA_prefix.get() > 1190043124 && DNA_prefix.get() <= 1197183664)
		  return 4;

		if(DNA_prefix.get() > 1197183664 && DNA_prefix.get() <= 1207519531)
		  return 3;

		if(DNA_prefix.get() > 1207519531 && DNA_prefix.get() <= 1214202172)
		  return 2;

		if(DNA_prefix.get() > 1214202172 && DNA_prefix.get() <= 1218476469)
		  return 1;

		return 0;
	}


	//if(DNA_prefix.get() <= 311504366)
	//  return 31;
	//
	//if(DNA_prefix.get() > 311504366 && DNA_prefix.get() <= 327835406)
	//  return 30;
	//
	//if(DNA_prefix.get() > 327835406 && DNA_prefix.get() <= 341762500)
	//  return 29;
	//
	//if(DNA_prefix.get() > 341762500 && DNA_prefix.get() <= 364841408)
	//  return 28;
	//
	//if(DNA_prefix.get() > 364841408 && DNA_prefix.get() <= 402921068)
	//  return 27;
	//
	//if(DNA_prefix.get() > 402921068 && DNA_prefix.get() <= 419372869)
	//  return 26;
	//
	//if(DNA_prefix.get() > 419372869 && DNA_prefix.get() <= 439168083)
	//  return 25;
	//
	//if(DNA_prefix.get() > 439168083 && DNA_prefix.get() <= 467915609)
	//  return 24;
	//
	//if(DNA_prefix.get() > 467915609 && DNA_prefix.get() <= 484270947)
	//  return 23;
	//
	//if(DNA_prefix.get() > 484270947 && DNA_prefix.get() <= 555178125)
	//  return 22;
	//
	//if(DNA_prefix.get() > 555178125 && DNA_prefix.get() <= 571238617)
	//  return 21;
	//
	//if(DNA_prefix.get() > 571238617 && DNA_prefix.get() <= 585851846)
	//  return 20;
	//
	//if(DNA_prefix.get() > 585851846 && DNA_prefix.get() <= 628624349)
	//  return 19;
	//
	//if(DNA_prefix.get() > 628624349 && DNA_prefix.get() <= 696083712)
	//  return 18;
	//
	//if(DNA_prefix.get() > 696083712 && DNA_prefix.get() <= 717466466)
	//  return 17;
	//
	//if(DNA_prefix.get() > 717466466 && DNA_prefix.get() <= 732233311)
	//  return 16;
	//
	//if(DNA_prefix.get() > 732233311 && DNA_prefix.get() <= 812991875)
	//  return 15;
	//
	//if(DNA_prefix.get() > 812991875 && DNA_prefix.get() <= 843457733)
	//  return 14;
	//
	//if(DNA_prefix.get() > 843457733 && DNA_prefix.get() <= 874941682)
	//  return 13;
	//
	//if(DNA_prefix.get() > 874941682 && DNA_prefix.get() <= 910671209)
	//  return 12;
	//
	//if(DNA_prefix.get() > 910671209 && DNA_prefix.get() <= 946988448)
	//  return 11;
	//
	//if(DNA_prefix.get() > 946988448 && DNA_prefix.get() <= 966792407)
	//  return 10;
	//
	//if(DNA_prefix.get() > 966792407 && DNA_prefix.get() <= 1040222795)
	//  return 9;
	//
	//if(DNA_prefix.get() > 1040222795 && DNA_prefix.get() <= 1063477809)
	//  return 8;
	//
	//if(DNA_prefix.get() > 1063477809 && DNA_prefix.get() <= 1090417683)
	//  return 7;
	//
	//if(DNA_prefix.get() > 1090417683 && DNA_prefix.get() <= 1117774786)
	//  return 6;
	//
	//if(DNA_prefix.get() > 1117774786 && DNA_prefix.get() <= 1141113323)
	//  return 5;
	//
	//if(DNA_prefix.get() > 1141113323 && DNA_prefix.get() <= 1160021869)
	//  return 4;
	//
	//if(DNA_prefix.get() > 1160021869 && DNA_prefix.get() <= 1171868486)
	//  return 3;
	//
	//if(DNA_prefix.get() > 1171868486 && DNA_prefix.get() <= 1197151871)
	//  return 2;
	//
	//if(DNA_prefix.get() > 1197151871 && DNA_prefix.get() <= 1214181037)
	//  return 1;
	//
	//return 0;

	//64G
	//System.out.print("hahahahahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

	//if(DNA_prefix.get() <= 307212483)
	//  return 63;
	//
	//if(DNA_prefix.get() > 307212483 && DNA_prefix.get() <= 311484034)
	//  return 62;
	//
	//if(DNA_prefix.get() > 311484034 && DNA_prefix.get() <= 317554618)
	//  return 61;
	//
	//if(DNA_prefix.get() > 317554618 && DNA_prefix.get() <= 327945239)
	//  return 60;
	//
	//if(DNA_prefix.get() > 327945239 && DNA_prefix.get() <= 335910236)
	//  return 59;
	//
	//if(DNA_prefix.get() > 335910236 && DNA_prefix.get() <= 341770174)
	//  return 58;
	//
	//if(DNA_prefix.get() > 341770174 && DNA_prefix.get() <= 358108749)
	//  return 57;
	//
	//if(DNA_prefix.get() > 358108749 && DNA_prefix.get() <= 364834334)
	//  return 56;
	//
	//if(DNA_prefix.get() > 364834334 && DNA_prefix.get() <= 384499846)
	//  return 55;
	//
	//if(DNA_prefix.get() > 384499846 && DNA_prefix.get() <= 402879606)
	//  return 54;
	//
	//if(DNA_prefix.get() > 402879606 && DNA_prefix.get() <= 409317749)
	//  return 53;
	//
	//if(DNA_prefix.get() > 409317749 && DNA_prefix.get() <= 419414814)
	//  return 52;
	//
	//if(DNA_prefix.get() > 419414814 && DNA_prefix.get() <= 432163961)
	//  return 51;
	//
	//if(DNA_prefix.get() > 432163961 && DNA_prefix.get() <= 439259047)
	//  return 50;
	//
	//if(DNA_prefix.get() > 439259047 && DNA_prefix.get() <= 457789219)
	//  return 49;
	//
	//if(DNA_prefix.get() > 457789219 && DNA_prefix.get() <= 468087284)
	//  return 48;
	//
	//if(DNA_prefix.get() > 468087284 && DNA_prefix.get() <= 477085468)
	//  return 47;
	//
	//if(DNA_prefix.get() > 477085468 && DNA_prefix.get() <= 484920934)
	//  return 46;
	//
	//if(DNA_prefix.get() > 484920934 && DNA_prefix.get() <= 549404557)
	//  return 45;
	//
	//if(DNA_prefix.get() > 549404557 && DNA_prefix.get() <= 555615288)
	//  return 44;
	//
	//if(DNA_prefix.get() > 555615288 && DNA_prefix.get() <= 564255624)
	//  return 43;
	//
	//if(DNA_prefix.get() > 564255624 && DNA_prefix.get() <= 571848736)
	//  return 42;
	//
	//if(DNA_prefix.get() > 571848736 && DNA_prefix.get() <= 580076222)
	//  return 41;
	//
	//if(DNA_prefix.get() > 580076222 && DNA_prefix.get() <= 585928109)
	//  return 40;
	//
	//if(DNA_prefix.get() > 585928109 && DNA_prefix.get() <= 605458359)
	//  return 39;
	//
	//if(DNA_prefix.get() > 605458359 && DNA_prefix.get() <= 628882249)
	//  return 38;
	//
	//if(DNA_prefix.get() > 628882249 && DNA_prefix.get() <= 654195294)
	//  return 37;
	//
	//if(DNA_prefix.get() > 654195294 && DNA_prefix.get() <= 696367438)
	//  return 36;
	//
	//if(DNA_prefix.get() > 696367438 && DNA_prefix.get() <= 708181036)
	//  return 35;
	//
	//if(DNA_prefix.get() > 708181036 && DNA_prefix.get() <= 717677696)
	//  return 34;
	//
	//if(DNA_prefix.get() > 717677696 && DNA_prefix.get() <= 725527784)
	//  return 33;
	//
	//if(DNA_prefix.get() > 725527784 && DNA_prefix.get() <= 732398059)
	//  return 32;
	//
	//if(DNA_prefix.get() > 732398059 && DNA_prefix.get() <= 799952971)
	//  return 31;
	//
	//if(DNA_prefix.get() > 799952971 && DNA_prefix.get() <= 813383533)
	//  return 30;
	//
	//if(DNA_prefix.get() > 813383533 && DNA_prefix.get() <= 823967093)
	//  return 29;
	//
	//if(DNA_prefix.get() > 823967093 && DNA_prefix.get() <= 844342743)
	//  return 28;
	//
	//if(DNA_prefix.get() > 844342743 && DNA_prefix.get() <= 854873933)
	//  return 27;
	//
	//if(DNA_prefix.get() > 854873933 && DNA_prefix.get() <= 875885821)
	//  return 26;
	//
	//if(DNA_prefix.get() > 875885821 && DNA_prefix.get() <= 895803939)
	//  return 25;
	//
	//if(DNA_prefix.get() > 895803939 && DNA_prefix.get() <= 911583586)
	//  return 24;
	//
	//if(DNA_prefix.get() > 911583586 && DNA_prefix.get() <= 926521448)
	//  return 23;
	//
	//if(DNA_prefix.get() > 926521448 && DNA_prefix.get() <= 949754371)
	//  return 22;
	//
	//if(DNA_prefix.get() > 949754371 && DNA_prefix.get() <= 960685833)
	//  return 21;
	//
	//if(DNA_prefix.get() > 960685833 && DNA_prefix.get() <= 969676872)
	//  return 20;
	//
	//if(DNA_prefix.get() > 969676872 && DNA_prefix.get() <= 976528981)
	//  return 19;
	//
	//if(DNA_prefix.get() > 976528981 && DNA_prefix.get() <= 1041666723)
	//  return 18;
	//
	//if(DNA_prefix.get() > 1041666723 && DNA_prefix.get() <= 1051318739)
	//  return 17;
	//
	//if(DNA_prefix.get() > 1051318739 && DNA_prefix.get() <= 1066406250)
	//  return 16;
	//
	//if(DNA_prefix.get() > 1066406250 && DNA_prefix.get() <= 1073929237)
	//  return 15;
	//
	//if(DNA_prefix.get() > 1073929237 && DNA_prefix.get() <= 1090994844)
	//  return 14;
	//
	//if(DNA_prefix.get() > 1090994844 && DNA_prefix.get() <= 1099421807)
	//  return 13;
	//
	//if(DNA_prefix.get() > 1099421807 && DNA_prefix.get() <= 1118667438)
	//  return 12;
	//
	//if(DNA_prefix.get() > 1118667438 && DNA_prefix.get() <= 1135519531)
	//  return 11;
	//
	//if(DNA_prefix.get() > 1135519531 && DNA_prefix.get() <= 1141755473)
	//  return 10;
	//
	//if(DNA_prefix.get() > 1141755473 && DNA_prefix.get() <= 1151535611)
	//  return 9;
	//
	//if(DNA_prefix.get() > 1151535611 && DNA_prefix.get() <= 1161123661)
	//  return 8;
	//
	//if(DNA_prefix.get() > 1161123661 && DNA_prefix.get() <= 1168740282)
	//  return 7;
	//
	//if(DNA_prefix.get() > 1168740282 && DNA_prefix.get() <= 1171875000)
	//  return 6;
	//
	//if(DNA_prefix.get() > 1171875000 && DNA_prefix.get() <= 1190229034)
	//  return 5;
	//
	//if(DNA_prefix.get() > 1190229034 && DNA_prefix.get() <= 1198165291)
	//  return 4;
	//
	//if(DNA_prefix.get() > 1198165291 && DNA_prefix.get() <= 1207568298)
	//  return 3;
	//
	//if(DNA_prefix.get() > 1207568298 && DNA_prefix.get() <= 1214265607)
	//  return 2;
	//
	//if(DNA_prefix.get() > 1214265607 && DNA_prefix.get() <= 1218505231)
	//  return 1;
	//
	//return 0;

	    
  }
}

