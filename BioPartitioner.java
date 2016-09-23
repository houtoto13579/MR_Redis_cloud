import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class BioPartitioner extends Partitioner<IntWritable,LongWritable> {
  @Override
  public int getPartition(IntWritable DNA_prefix, LongWritable value, int numReduceTasks) {
  //public int getPartition(LongWritable DNA_prefix, LongWritable value, int numReduceTasks) {
      //13 chars for 64 reducers      
      //if(DNA_prefix.get() <= 307286598)
      //  return 0;
      //
      //if(DNA_prefix.get() > 307286598 && DNA_prefix.get() <= 311590374)
      //  return 1;
      //
      //if(DNA_prefix.get() > 311590374 && DNA_prefix.get() <= 317679796)
      //  return 2;
      //
      //if(DNA_prefix.get() > 317679796 && DNA_prefix.get() <= 327879931)
      //  return 3;
      //
      //if(DNA_prefix.get() > 327879931 && DNA_prefix.get() <= 335919872)
      //  return 4;
      //
      //if(DNA_prefix.get() > 335919872 && DNA_prefix.get() <= 341770207)
      //  return 5;
      //
      //if(DNA_prefix.get() > 341770207 && DNA_prefix.get() <= 358098743)
      //  return 6;
      //
      //if(DNA_prefix.get() > 358098743 && DNA_prefix.get() <= 364738281)
      //  return 7;
      //
      //if(DNA_prefix.get() > 364738281 && DNA_prefix.get() <= 384318675)
      //  return 8;
      //
      //if(DNA_prefix.get() > 384318675 && DNA_prefix.get() <= 402832281)
      //  return 9;
      //
      //if(DNA_prefix.get() > 402832281 && DNA_prefix.get() <= 409084334)
      //  return 10;
      //
      //if(DNA_prefix.get() > 409084334 && DNA_prefix.get() <= 419358617)
      //  return 11;
      //
      //if(DNA_prefix.get() > 419358617 && DNA_prefix.get() <= 429687500)
      //  return 12;
      //
      //if(DNA_prefix.get() > 429687500 && DNA_prefix.get() <= 439179561)
      //  return 13;
      //
      //if(DNA_prefix.get() > 439179561 && DNA_prefix.get() <= 457763671)
      //  return 14;
      //
      //if(DNA_prefix.get() > 457763671 && DNA_prefix.get() <= 468071849)
      //  return 15;
      //
      //if(DNA_prefix.get() > 468071849 && DNA_prefix.get() <= 477090306)
      //  return 16;
      //
      //if(DNA_prefix.get() > 477090306 && DNA_prefix.get() <= 484317449)
      //  return 17;
      //
      //if(DNA_prefix.get() > 484317449 && DNA_prefix.get() <= 549531250)
      //  return 18;
      //
      //if(DNA_prefix.get() > 549531250 && DNA_prefix.get() <= 555843658)
      //  return 19;
      //
      //if(DNA_prefix.get() > 555843658 && DNA_prefix.get() <= 564952784)
      //  return 20;
      //
      //if(DNA_prefix.get() > 564952784 && DNA_prefix.get() <= 572057997)
      //  return 21;
      //
      //if(DNA_prefix.get() > 572057997 && DNA_prefix.get() <= 580062494)
      //  return 22;
      //
      //if(DNA_prefix.get() > 580062494 && DNA_prefix.get() <= 585898344)
      //  return 23;
      //
      //if(DNA_prefix.get() > 585898344 && DNA_prefix.get() <= 605311166)
      //  return 24;
      //
      //if(DNA_prefix.get() > 605311166 && DNA_prefix.get() <= 628482948)
      //  return 25;
      //
      //if(DNA_prefix.get() > 628482948 && DNA_prefix.get() <= 652265544)
      //  return 26;
      //
      //if(DNA_prefix.get() > 652265544 && DNA_prefix.get() <= 696279186)
      //  return 27;
      //
      //if(DNA_prefix.get() > 696279186 && DNA_prefix.get() <= 708012067)
      //  return 28;
      //
      //if(DNA_prefix.get() > 708012067 && DNA_prefix.get() <= 717529375)
      //  return 29;
      //
      //if(DNA_prefix.get() > 717529375 && DNA_prefix.get() <= 725382186)
      //  return 30;
      //
      //if(DNA_prefix.get() > 725382186 && DNA_prefix.get() <= 732317042)
      //  return 31;
      //
      //if(DNA_prefix.get() > 732317042 && DNA_prefix.get() <= 799609231)
      //  return 32;
      //
      //if(DNA_prefix.get() > 799609231 && DNA_prefix.get() <= 813170408)
      //  return 33;
      //
      //if(DNA_prefix.get() > 813170408 && DNA_prefix.get() <= 823779048)
      //  return 34;
      //
      //if(DNA_prefix.get() > 823779048 && DNA_prefix.get() <= 844327799)
      //  return 35;
      //
      //if(DNA_prefix.get() > 844327799 && DNA_prefix.get() <= 854432822)
      //  return 36;
      //
      //if(DNA_prefix.get() > 854432822 && DNA_prefix.get() <= 875676624)
      //  return 37;
      //
      //if(DNA_prefix.get() > 875676624 && DNA_prefix.get() <= 895445623)
      //  return 38;
      //
      //if(DNA_prefix.get() > 895445623 && DNA_prefix.get() <= 911233333)
      //  return 39;
      //
      //if(DNA_prefix.get() > 911233333 && DNA_prefix.get() <= 925686239)
      //  return 40;
      //
      //if(DNA_prefix.get() > 925686239 && DNA_prefix.get() <= 947185616)
      //  return 41;
      //
      //if(DNA_prefix.get() > 947185616 && DNA_prefix.get() <= 960289744)
      //  return 42;
      //
      //if(DNA_prefix.get() > 960289744 && DNA_prefix.get() <= 969240499)
      //  return 43;
      //
      //if(DNA_prefix.get() > 969240499 && DNA_prefix.get() <= 976290282)
      //  return 44;
      //
      //if(DNA_prefix.get() > 976290282 && DNA_prefix.get() <= 1041933374)
      //  return 45;
      //
      //if(DNA_prefix.get() > 1041933374 && DNA_prefix.get() <= 1052656244)
      //  return 46;
      //
      //if(DNA_prefix.get() > 1052656244 && DNA_prefix.get() <= 1067024914)
      //  return 47;
      //
      //if(DNA_prefix.get() > 1067024914 && DNA_prefix.get() <= 1074010494)
      //  return 48;
      //
      //if(DNA_prefix.get() > 1074010494 && DNA_prefix.get() <= 1091244866)
      //  return 49;
      //
      //if(DNA_prefix.get() > 1091244866 && DNA_prefix.get() <= 1099607699)
      //  return 50;
      //
      //if(DNA_prefix.get() > 1099607699 && DNA_prefix.get() <= 1118863731)
      //  return 51;
      //
      //if(DNA_prefix.get() > 1118863731 && DNA_prefix.get() <= 1135681037)
      //  return 52;
      //
      //if(DNA_prefix.get() > 1135681037 && DNA_prefix.get() <= 1141904034)
      //  return 53;
      //
      //if(DNA_prefix.get() > 1141904034 && DNA_prefix.get() <= 1151668484)
      //  return 54;
      //
      //if(DNA_prefix.get() > 1151668484 && DNA_prefix.get() <= 1161052739)
      //  return 55;
      //
      //if(DNA_prefix.get() > 1161052739 && DNA_prefix.get() <= 1168651491)
      //  return 56;
      //
      //if(DNA_prefix.get() > 1168651491 && DNA_prefix.get() <= 1184102946)
      //  return 57;
      //
      //if(DNA_prefix.get() > 1184102946 && DNA_prefix.get() <= 1190349043)
      //  return 58;
      //
      //if(DNA_prefix.get() > 1190349043 && DNA_prefix.get() <= 1198341709)
      //  return 59;
      //
      //if(DNA_prefix.get() > 1198341709 && DNA_prefix.get() <= 1207618411)
      //  return 60;
      //
      //if(DNA_prefix.get() > 1207618411 && DNA_prefix.get() <= 1214370839)
      //  return 61;
      //
      //if(DNA_prefix.get() > 1214370839 && DNA_prefix.get() <= 1218545609)
      //  return 62;
      //
      //return 63;
      /*** for comparison with new partition ***/
      //if(DNA_prefix.get() <= 307327799)
      //  return 0;
      //
      //if(DNA_prefix.get() > 307327799 && DNA_prefix.get() <= 311708997)
      //  return 1;
      //
      //if(DNA_prefix.get() > 311708997 && DNA_prefix.get() <= 318171848)
      //  return 2;
      //
      //if(DNA_prefix.get() > 318171848 && DNA_prefix.get() <= 327988709)
      //  return 3;
      //
      //if(DNA_prefix.get() > 327988709 && DNA_prefix.get() <= 335936234)
      //  return 4;
      //
      //if(DNA_prefix.get() > 335936234 && DNA_prefix.get() <= 341788687)
      //  return 5;
      //
      //if(DNA_prefix.get() > 341788687 && DNA_prefix.get() <= 358160543)
      //  return 6;
      //
      //if(DNA_prefix.get() > 358160543 && DNA_prefix.get() <= 364946724)
      //  return 7;
      //
      //if(DNA_prefix.get() > 364946724 && DNA_prefix.get() <= 384507919)
      //  return 8;
      //
      //if(DNA_prefix.get() > 384507919 && DNA_prefix.get() <= 402849998)
      //  return 9;
      //
      //if(DNA_prefix.get() > 402849998 && DNA_prefix.get() <= 409210412)
      //  return 10;
      //
      //if(DNA_prefix.get() > 409210412 && DNA_prefix.get() <= 419429069)
      //  return 11;
      //
      //if(DNA_prefix.get() > 419429069 && DNA_prefix.get() <= 432149983)
      //  return 12;
      //
      //if(DNA_prefix.get() > 432149983 && DNA_prefix.get() <= 439216724)
      //  return 13;
      //
      //if(DNA_prefix.get() > 439216724 && DNA_prefix.get() <= 457763674)
      //  return 14;
      //
      //if(DNA_prefix.get() > 457763674 && DNA_prefix.get() <= 468092741)
      //  return 15;
      //
      //if(DNA_prefix.get() > 468092741 && DNA_prefix.get() <= 477078047)
      //  return 16;
      //
      //if(DNA_prefix.get() > 477078047 && DNA_prefix.get() <= 484240419)
      //  return 17;
      //
      //if(DNA_prefix.get() > 484240419 && DNA_prefix.get() <= 549479749)
      //  return 18;
      //
      //if(DNA_prefix.get() > 549479749 && DNA_prefix.get() <= 555661583)
      //  return 19;
      //
      //if(DNA_prefix.get() > 555661583 && DNA_prefix.get() <= 564449657)
      //  return 20;
      //
      //if(DNA_prefix.get() > 564449657 && DNA_prefix.get() <= 571989783)
      //  return 21;
      //
      //if(DNA_prefix.get() > 571989783 && DNA_prefix.get() <= 580024963)
      //  return 22;
      //
      //if(DNA_prefix.get() > 580024963 && DNA_prefix.get() <= 585882409)
      //  return 23;
      //
      //if(DNA_prefix.get() > 585882409 && DNA_prefix.get() <= 605256250)
      //  return 24;
      //
      //if(DNA_prefix.get() > 605256250 && DNA_prefix.get() <= 628387467)
      //  return 25;
      //
      //if(DNA_prefix.get() > 628387467 && DNA_prefix.get() <= 652137411)
      //  return 26;
      //
      //if(DNA_prefix.get() > 652137411 && DNA_prefix.get() <= 696247932)
      //  return 27;
      //
      //if(DNA_prefix.get() > 696247932 && DNA_prefix.get() <= 707968036)
      //  return 28;
      //
      //if(DNA_prefix.get() > 707968036 && DNA_prefix.get() <= 717499749)
      //  return 29;
      //
      //if(DNA_prefix.get() > 717499749 && DNA_prefix.get() <= 725352822)
      //  return 30;
      //
      //if(DNA_prefix.get() > 725352822 && DNA_prefix.get() <= 732290599)
      //  return 31;
      //
      //if(DNA_prefix.get() > 732290599 && DNA_prefix.get() <= 799582407)
      //  return 32;
      //
      //if(DNA_prefix.get() > 799582407 && DNA_prefix.get() <= 813155781)
      //  return 33;
      //
      //if(DNA_prefix.get() > 813155781 && DNA_prefix.get() <= 823742036)
      //  return 34;
      //
      //if(DNA_prefix.get() > 823742036 && DNA_prefix.get() <= 844337332)
      //  return 35;
      //
      //if(DNA_prefix.get() > 844337332 && DNA_prefix.get() <= 854507091)
      //  return 36;
      //
      //if(DNA_prefix.get() > 854507091 && DNA_prefix.get() <= 875722824)
      //  return 37;
      //
      //if(DNA_prefix.get() > 875722824 && DNA_prefix.get() <= 895495361)
      //  return 38;
      //
      //if(DNA_prefix.get() > 895495361 && DNA_prefix.get() <= 911443743)
      //  return 39;
      //
      //if(DNA_prefix.get() > 911443743 && DNA_prefix.get() <= 926271412)
      //  return 40;
      //
      //if(DNA_prefix.get() > 926271412 && DNA_prefix.get() <= 947261808)
      //  return 41;
      //
      //if(DNA_prefix.get() > 947261808 && DNA_prefix.get() <= 960417443)
      //  return 42;
      //
      //if(DNA_prefix.get() > 960417443 && DNA_prefix.get() <= 969296843)
      //  return 43;
      //
      //if(DNA_prefix.get() > 969296843 && DNA_prefix.get() <= 976324368)
      //  return 44;
      //
      //if(DNA_prefix.get() > 976324368 && DNA_prefix.get() <= 1042169808)
      //  return 45;
      //
      //if(DNA_prefix.get() > 1042169808 && DNA_prefix.get() <= 1053260983)
      //  return 46;
      //
      //if(DNA_prefix.get() > 1053260983 && DNA_prefix.get() <= 1067105213)
      //  return 47;
      //
      //if(DNA_prefix.get() > 1067105213 && DNA_prefix.get() <= 1074029356)
      //  return 48;
      //
      //if(DNA_prefix.get() > 1074029356 && DNA_prefix.get() <= 1091295922)
      //  return 49;
      //
      //if(DNA_prefix.get() > 1091295922 && DNA_prefix.get() <= 1100139223)
      //  return 50;
      //
      //if(DNA_prefix.get() > 1100139223 && DNA_prefix.get() <= 1118929166)
      //  return 51;
      //
      //if(DNA_prefix.get() > 1118929166 && DNA_prefix.get() <= 1135738296)
      //  return 52;
      //
      //if(DNA_prefix.get() > 1135738296 && DNA_prefix.get() <= 1141948084)
      //  return 53;
      //
      //if(DNA_prefix.get() > 1141948084 && DNA_prefix.get() <= 1151696171)
      //  return 54;
      //
      //if(DNA_prefix.get() > 1151696171 && DNA_prefix.get() <= 1161163418)
      //  return 55;
      //
      //if(DNA_prefix.get() > 1161163418 && DNA_prefix.get() <= 1168702783)
      //  return 56;
      //
      //if(DNA_prefix.get() > 1168702783 && DNA_prefix.get() <= 1184136856)
      //  return 57;
      //
      //if(DNA_prefix.get() > 1184136856 && DNA_prefix.get() <= 1190463662)
      //  return 58;
      //
      //if(DNA_prefix.get() > 1190463662 && DNA_prefix.get() <= 1198795624)
      //  return 59;
      //
      //if(DNA_prefix.get() > 1198795624 && DNA_prefix.get() <= 1207652092)
      //  return 60;
      //
      //if(DNA_prefix.get() > 1207652092 && DNA_prefix.get() <= 1214342816)
      //  return 61;
      //
      //if(DNA_prefix.get() > 1214342816 && DNA_prefix.get() <= 1218510789)
      //  return 62;
      //
      //return 63;
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


if(DNA_prefix.get() <= 307212483)
  return 63;

if(DNA_prefix.get() > 307212483 && DNA_prefix.get() <= 311484034)
  return 62;

if(DNA_prefix.get() > 311484034 && DNA_prefix.get() <= 317554618)
  return 61;

if(DNA_prefix.get() > 317554618 && DNA_prefix.get() <= 327945239)
  return 60;

if(DNA_prefix.get() > 327945239 && DNA_prefix.get() <= 335910236)
  return 59;

if(DNA_prefix.get() > 335910236 && DNA_prefix.get() <= 341770174)
  return 58;

if(DNA_prefix.get() > 341770174 && DNA_prefix.get() <= 358108749)
  return 57;

if(DNA_prefix.get() > 358108749 && DNA_prefix.get() <= 364834334)
  return 56;

if(DNA_prefix.get() > 364834334 && DNA_prefix.get() <= 384499846)
  return 55;

if(DNA_prefix.get() > 384499846 && DNA_prefix.get() <= 402879606)
  return 54;

if(DNA_prefix.get() > 402879606 && DNA_prefix.get() <= 409317749)
  return 53;

if(DNA_prefix.get() > 409317749 && DNA_prefix.get() <= 419414814)
  return 52;

if(DNA_prefix.get() > 419414814 && DNA_prefix.get() <= 432163961)
  return 51;

if(DNA_prefix.get() > 432163961 && DNA_prefix.get() <= 439259047)
  return 50;

if(DNA_prefix.get() > 439259047 && DNA_prefix.get() <= 457789219)
  return 49;

if(DNA_prefix.get() > 457789219 && DNA_prefix.get() <= 468087284)
  return 48;

if(DNA_prefix.get() > 468087284 && DNA_prefix.get() <= 477085468)
  return 47;

if(DNA_prefix.get() > 477085468 && DNA_prefix.get() <= 484920934)
  return 46;

if(DNA_prefix.get() > 484920934 && DNA_prefix.get() <= 549404557)
  return 45;

if(DNA_prefix.get() > 549404557 && DNA_prefix.get() <= 555615288)
  return 44;

if(DNA_prefix.get() > 555615288 && DNA_prefix.get() <= 564255624)
  return 43;

if(DNA_prefix.get() > 564255624 && DNA_prefix.get() <= 571848736)
  return 42;

if(DNA_prefix.get() > 571848736 && DNA_prefix.get() <= 580076222)
  return 41;

if(DNA_prefix.get() > 580076222 && DNA_prefix.get() <= 585928109)
  return 40;

if(DNA_prefix.get() > 585928109 && DNA_prefix.get() <= 605458359)
  return 39;

if(DNA_prefix.get() > 605458359 && DNA_prefix.get() <= 628882249)
  return 38;

if(DNA_prefix.get() > 628882249 && DNA_prefix.get() <= 654195294)
  return 37;

if(DNA_prefix.get() > 654195294 && DNA_prefix.get() <= 696367438)
  return 36;

if(DNA_prefix.get() > 696367438 && DNA_prefix.get() <= 708181036)
  return 35;

if(DNA_prefix.get() > 708181036 && DNA_prefix.get() <= 717677696)
  return 34;

if(DNA_prefix.get() > 717677696 && DNA_prefix.get() <= 725527784)
  return 33;

if(DNA_prefix.get() > 725527784 && DNA_prefix.get() <= 732398059)
  return 32;

if(DNA_prefix.get() > 732398059 && DNA_prefix.get() <= 799952971)
  return 31;

if(DNA_prefix.get() > 799952971 && DNA_prefix.get() <= 813383533)
  return 30;

if(DNA_prefix.get() > 813383533 && DNA_prefix.get() <= 823967093)
  return 29;

if(DNA_prefix.get() > 823967093 && DNA_prefix.get() <= 844342743)
  return 28;

if(DNA_prefix.get() > 844342743 && DNA_prefix.get() <= 854873933)
  return 27;

if(DNA_prefix.get() > 854873933 && DNA_prefix.get() <= 875885821)
  return 26;

if(DNA_prefix.get() > 875885821 && DNA_prefix.get() <= 895803939)
  return 25;

if(DNA_prefix.get() > 895803939 && DNA_prefix.get() <= 911583586)
  return 24;

if(DNA_prefix.get() > 911583586 && DNA_prefix.get() <= 926521448)
  return 23;

if(DNA_prefix.get() > 926521448 && DNA_prefix.get() <= 949754371)
  return 22;

if(DNA_prefix.get() > 949754371 && DNA_prefix.get() <= 960685833)
  return 21;

if(DNA_prefix.get() > 960685833 && DNA_prefix.get() <= 969676872)
  return 20;

if(DNA_prefix.get() > 969676872 && DNA_prefix.get() <= 976528981)
  return 19;

if(DNA_prefix.get() > 976528981 && DNA_prefix.get() <= 1041666723)
  return 18;

if(DNA_prefix.get() > 1041666723 && DNA_prefix.get() <= 1051318739)
  return 17;

if(DNA_prefix.get() > 1051318739 && DNA_prefix.get() <= 1066406250)
  return 16;

if(DNA_prefix.get() > 1066406250 && DNA_prefix.get() <= 1073929237)
  return 15;

if(DNA_prefix.get() > 1073929237 && DNA_prefix.get() <= 1090994844)
  return 14;

if(DNA_prefix.get() > 1090994844 && DNA_prefix.get() <= 1099421807)
  return 13;

if(DNA_prefix.get() > 1099421807 && DNA_prefix.get() <= 1118667438)
  return 12;

if(DNA_prefix.get() > 1118667438 && DNA_prefix.get() <= 1135519531)
  return 11;

if(DNA_prefix.get() > 1135519531 && DNA_prefix.get() <= 1141755473)
  return 10;

if(DNA_prefix.get() > 1141755473 && DNA_prefix.get() <= 1151535611)
  return 9;

if(DNA_prefix.get() > 1151535611 && DNA_prefix.get() <= 1161123661)
  return 8;

if(DNA_prefix.get() > 1161123661 && DNA_prefix.get() <= 1168740282)
  return 7;

if(DNA_prefix.get() > 1168740282 && DNA_prefix.get() <= 1171875000)
  return 6;

if(DNA_prefix.get() > 1171875000 && DNA_prefix.get() <= 1190229034)
  return 5;

if(DNA_prefix.get() > 1190229034 && DNA_prefix.get() <= 1198165291)
  return 4;

if(DNA_prefix.get() > 1198165291 && DNA_prefix.get() <= 1207568298)
  return 3;

if(DNA_prefix.get() > 1207568298 && DNA_prefix.get() <= 1214265607)
  return 2;

if(DNA_prefix.get() > 1214265607 && DNA_prefix.get() <= 1218505231)
  return 1;

return 0;

      //13 chars
      //if(DNA_prefix.get() <= 311692847)
      //  return 0;
      //
      //if(DNA_prefix.get() > 311692847 && DNA_prefix.get() <= 328035871)
      //  return 1;
      //
      //if(DNA_prefix.get() > 328035871 && DNA_prefix.get() <= 354020996)
      //  return 2;
      //
      //if(DNA_prefix.get() > 354020996 && DNA_prefix.get() <= 365187500)
      //  return 3;
      //
      //if(DNA_prefix.get() > 365187500 && DNA_prefix.get() <= 402933531)
      //  return 4;
      //
      //if(DNA_prefix.get() > 402933531 && DNA_prefix.get() <= 419718289)
      //  return 5;
      //
      //if(DNA_prefix.get() > 419718289 && DNA_prefix.get() <= 439265291)
      //  return 6;
      //
      //if(DNA_prefix.get() > 439265291 && DNA_prefix.get() <= 468276617)
      //  return 7;
      //
      //if(DNA_prefix.get() > 468276617 && DNA_prefix.get() <= 485059163)
      //  return 8;
      //
      //if(DNA_prefix.get() > 485059163 && DNA_prefix.get() <= 556052858)
      //  return 9;
      //
      //if(DNA_prefix.get() > 556052858 && DNA_prefix.get() <= 572160167)
      //  return 10;
      //
      //if(DNA_prefix.get() > 572160167 && DNA_prefix.get() <= 598186536)
      //  return 11;
      //
      //if(DNA_prefix.get() > 598186536 && DNA_prefix.get() <= 629528000)
      //  return 12;
      //
      //if(DNA_prefix.get() > 629528000 && DNA_prefix.get() <= 696473667)
      //  return 13;
      //
      //if(DNA_prefix.get() > 696473667 && DNA_prefix.get() <= 717542797)
      //  return 14;
      //
      //if(DNA_prefix.get() > 717542797 && DNA_prefix.get() <= 732320241)
      //  return 15;
      //
      //if(DNA_prefix.get() > 732320241 && DNA_prefix.get() <= 813077669)
      //  return 16;
      //
      //if(DNA_prefix.get() > 813077669 && DNA_prefix.get() <= 844270289)
      //  return 17;
      //
      //if(DNA_prefix.get() > 844270289 && DNA_prefix.get() <= 875670981)
      //  return 18;
      //
      //if(DNA_prefix.get() > 875670981 && DNA_prefix.get() <= 911326463)
      //  return 19;
      //
      //if(DNA_prefix.get() > 911326463 && DNA_prefix.get() <= 947255975)
      //  return 20;
      //
      //if(DNA_prefix.get() > 947255975 && DNA_prefix.get() <= 969327696)
      //  return 21;
      //
      //if(DNA_prefix.get() > 969327696 && DNA_prefix.get() <= 1042134036)
      //  return 22;
      //
      //if(DNA_prefix.get() > 1042134036 && DNA_prefix.get() <= 1067022659)
      //  return 23;
      //
      //if(DNA_prefix.get() > 1067022659 && DNA_prefix.get() <= 1091319662)
      //  return 24;
      //
      //if(DNA_prefix.get() > 1091319662 && DNA_prefix.get() <= 1118945437)
      //  return 25;
      //
      //if(DNA_prefix.get() > 1118945437 && DNA_prefix.get() <= 1141718689)
      //  return 26;
      //
      //if(DNA_prefix.get() > 1141718689 && DNA_prefix.get() <= 1160858359)
      //  return 27;
      //
      //if(DNA_prefix.get() > 1160858359 && DNA_prefix.get() <= 1184130562)
      //  return 28;
      //
      //if(DNA_prefix.get() > 1184130562 && DNA_prefix.get() <= 1198769543)
      //  return 29;
      //
      //if(DNA_prefix.get() > 1198769543 && DNA_prefix.get() <= 1214415624)
      //  return 30;
      //
      //return 31;

      //13 chars sampled from small file
      //if(DNA_prefix.get() <= 311271531)
      //  return 0;
      //
      //if(DNA_prefix.get() > 311271531 && DNA_prefix.get() <= 327618731)
      //  return 1;
      //
      //if(DNA_prefix.get() > 327618731 && DNA_prefix.get() <= 341796875)
      //  return 2;
      //
      //if(DNA_prefix.get() > 341796875 && DNA_prefix.get() <= 366011873)
      //  return 3;
      //
      //if(DNA_prefix.get() > 366011873 && DNA_prefix.get() <= 403613724)
      //  return 4;
      //
      //if(DNA_prefix.get() > 403613724 && DNA_prefix.get() <= 422265625)
      //  return 5;
      //
      //if(DNA_prefix.get() > 422265625 && DNA_prefix.get() <= 451679589)
      //  return 6;
      //
      //if(DNA_prefix.get() > 451679589 && DNA_prefix.get() <= 468671482)
      //  return 7;
      //
      //if(DNA_prefix.get() > 468671482 && DNA_prefix.get() <= 485312036)
      //  return 8;
      //
      //if(DNA_prefix.get() > 485312036 && DNA_prefix.get() <= 555724607)
      //  return 9;
      //
      //if(DNA_prefix.get() > 555724607 && DNA_prefix.get() <= 572769918)
      //  return 10;
      //
      //if(DNA_prefix.get() > 572769918 && DNA_prefix.get() <= 598302434)
      //  return 11;
      //
      //if(DNA_prefix.get() > 598302434 && DNA_prefix.get() <= 629841442)
      //  return 12;
      //
      //if(DNA_prefix.get() > 629841442 && DNA_prefix.get() <= 697786158)
      //  return 13;
      //
      //if(DNA_prefix.get() > 697786158 && DNA_prefix.get() <= 718638532)
      //  return 14;
      //
      //if(DNA_prefix.get() > 718638532 && DNA_prefix.get() <= 793593749)
      //  return 15;
      //
      //if(DNA_prefix.get() > 793593749 && DNA_prefix.get() <= 814218122)
      //  return 16;
      //
      //if(DNA_prefix.get() > 814218122 && DNA_prefix.get() <= 845699614)
      //  return 17;
      //
      //if(DNA_prefix.get() > 845699614 && DNA_prefix.get() <= 876373714)
      //  return 18;
      //
      //if(DNA_prefix.get() > 876373714 && DNA_prefix.get() <= 911984367)
      //  return 19;
      //
      //if(DNA_prefix.get() > 911984367 && DNA_prefix.get() <= 950257836)
      //  return 20;
      //
      //if(DNA_prefix.get() > 950257836 && DNA_prefix.get() <= 970106224)
      //  return 21;
      //
      //if(DNA_prefix.get() > 970106224 && DNA_prefix.get() <= 1042910625)
      //  return 22;
      //
      //if(DNA_prefix.get() > 1042910625 && DNA_prefix.get() <= 1067764944)
      //  return 23;
      //
      //if(DNA_prefix.get() > 1067764944 && DNA_prefix.get() <= 1091546056)
      //  return 24;
      //
      //if(DNA_prefix.get() > 1091546056 && DNA_prefix.get() <= 1119635157)
      //  return 25;
      //
      //if(DNA_prefix.get() > 1119635157 && DNA_prefix.get() <= 1142265625)
      //  return 26;
      //
      //if(DNA_prefix.get() > 1142265625 && DNA_prefix.get() <= 1161295596)
      //  return 27;
      //
      //if(DNA_prefix.get() > 1161295596 && DNA_prefix.get() <= 1171875000)
      //  return 28;
      //
      //if(DNA_prefix.get() > 1171875000 && DNA_prefix.get() <= 1197216499)
      //  return 29;
      //
      //if(DNA_prefix.get() > 1197216499 && DNA_prefix.get() <= 1214265347)
      //  return 30;
      //
      //return 31; 


      //12 chars
      //if(DNA_prefix.get() < 62289617)
      //  return 0;
      //
      //if(DNA_prefix.get() >= 62289617 && DNA_prefix.get() < 65492050)
      //  return 1;
      //
      //if(DNA_prefix.get() >= 65492050 && DNA_prefix.get() < 68332848)
      //  return 2;
      //
      //if(DNA_prefix.get() >= 68332848 && DNA_prefix.get() < 72941707)
      //  return 3;
      //
      //if(DNA_prefix.get() >= 72941707 && DNA_prefix.get() < 80576558)
      //  return 4;
      //
      //if(DNA_prefix.get() >= 80576558 && DNA_prefix.get() < 83904973)
      //  return 5;
      //
      //if(DNA_prefix.get() >= 83904973 && DNA_prefix.get() < 87858625)
      //  return 6;
      //
      //if(DNA_prefix.get() >= 87858625 && DNA_prefix.get() < 93590448)
      //  return 7;
      //
      //if(DNA_prefix.get() >= 93590448 && DNA_prefix.get() < 96974243)
      //  return 8;
      //
      //if(DNA_prefix.get() >= 96974243 && DNA_prefix.get() < 111086484)
      //  return 9;
      //
      //if(DNA_prefix.get() >= 111086484 && DNA_prefix.get() < 114412356)
      //  return 10;
      //
      //if(DNA_prefix.get() >= 114412356 && DNA_prefix.get() < 117187500)
      //  return 11;
      //
      //if(DNA_prefix.get() >= 117187500 && DNA_prefix.get() < 125879536)
      //  return 12;
      //
      //if(DNA_prefix.get() >= 125879536 && DNA_prefix.get() < 139337369)
      //  return 13;
      //
      //if(DNA_prefix.get() >= 139337369 && DNA_prefix.get() < 143587499)
      //  return 14;
      //
      //if(DNA_prefix.get() >= 143587499 && DNA_prefix.get() < 158593750)
      //  return 15;
      //
      //if(DNA_prefix.get() >= 158593750 && DNA_prefix.get() < 162756061)
      //  return 16;
      //
      //if(DNA_prefix.get() >= 162756061 && DNA_prefix.get() < 168974932)
      //  return 17;
      //
      //if(DNA_prefix.get() >= 168974932 && DNA_prefix.get() < 175202496)
      //  return 18;
      //
      //if(DNA_prefix.get() >= 175202496 && DNA_prefix.get() < 182323933)
      //  return 19;
      //
      //if(DNA_prefix.get() >= 182323933 && DNA_prefix.get() < 189951832)
      //  return 20;
      //
      //if(DNA_prefix.get() >= 189951832 && DNA_prefix.get() < 193927657)
      //  return 21;
      //
      //if(DNA_prefix.get() >= 193927657 && DNA_prefix.get() < 208339036)
      //  return 22;
      //
      //if(DNA_prefix.get() >= 208339036 && DNA_prefix.get() < 213378916)
      //  return 23;
      //
      //if(DNA_prefix.get() >= 213378916 && DNA_prefix.get() < 218193347)
      //  return 24;
      //
      //if(DNA_prefix.get() >= 218193347 && DNA_prefix.get() < 223668617)
      //  return 25;
      //
      //if(DNA_prefix.get() >= 223668617 && DNA_prefix.get() < 228310157)
      //  return 26;
      //
      //if(DNA_prefix.get() >= 228310157 && DNA_prefix.get() < 232168361)
      //  return 27;
      //
      //if(DNA_prefix.get() >= 232168361 && DNA_prefix.get() < 236817282)
      //  return 28;
      //
      //if(DNA_prefix.get() >= 236817282 && DNA_prefix.get() < 239452409)
      //  return 29;
      //
      //if(DNA_prefix.get() >= 239452409 && DNA_prefix.get() < 242863319)
      //  return 30;
      //
      //return 31;

      
      //10 chars
      //if(DNA_prefix.get() <= 2490172)
      //  return 0;
      //
      //if(DNA_prefix.get() > 2490172 && DNA_prefix.get() <= 2620949)
      //  return 1;
      //
      //if(DNA_prefix.get() > 2620949 && DNA_prefix.get() <= 2734375)
      //  return 2;
      //
      //if(DNA_prefix.get() > 2734375 && DNA_prefix.get() <= 2928094)
      //  return 3;
      //
      //if(DNA_prefix.get() > 2928094 && DNA_prefix.get() <= 3228909)
      //  return 4;
      //
      //if(DNA_prefix.get() > 3228909 && DNA_prefix.get() <= 3378125)
      //  return 5;
      //
      //if(DNA_prefix.get() > 3378125 && DNA_prefix.get() <= 3613436)
      //  return 6;
      //
      //if(DNA_prefix.get() > 3613436 && DNA_prefix.get() <= 3749371)
      //  return 7;
      //
      //if(DNA_prefix.get() > 3749371 && DNA_prefix.get() <= 3882496)
      //  return 8;
      //
      //if(DNA_prefix.get() > 3882496 && DNA_prefix.get() <= 4445796)
      //  return 9;
      //
      //if(DNA_prefix.get() > 4445796 && DNA_prefix.get() <= 4582159)
      //  return 10;
      //
      //if(DNA_prefix.get() > 4582159 && DNA_prefix.get() <= 4786419)
      //  return 11;
      //
      //if(DNA_prefix.get() > 4786419 && DNA_prefix.get() <= 5038731)
      //  return 12;
      //
      //if(DNA_prefix.get() > 5038731 && DNA_prefix.get() <= 5582289)
      //  return 13;
      //
      //if(DNA_prefix.get() > 5582289 && DNA_prefix.get() <= 5749108)
      //  return 14;
      //
      //if(DNA_prefix.get() > 5749108 && DNA_prefix.get() <= 6348749)
      //  return 15;
      //
      //if(DNA_prefix.get() > 6348749 && DNA_prefix.get() <= 6513744)
      //  return 16;
      //
      //if(DNA_prefix.get() > 6513744 && DNA_prefix.get() <= 6765596)
      //  return 17;
      //
      //if(DNA_prefix.get() > 6765596 && DNA_prefix.get() <= 7010989)
      //  return 18;
      //
      //if(DNA_prefix.get() > 7010989 && DNA_prefix.get() <= 7295874)
      //  return 19;
      //
      //if(DNA_prefix.get() > 7295874 && DNA_prefix.get() <= 7602062)
      //  return 20;
      //
      //if(DNA_prefix.get() > 7602062 && DNA_prefix.get() <= 7760849)
      //  return 21;
      //
      //if(DNA_prefix.get() > 7760849 && DNA_prefix.get() <= 8343285)
      //  return 22;
      //
      //if(DNA_prefix.get() > 8343285 && DNA_prefix.get() <= 8542119)
      //  return 23;
      //
      //if(DNA_prefix.get() > 8542119 && DNA_prefix.get() <= 8732368)
      //  return 24;
      //
      //if(DNA_prefix.get() > 8732368 && DNA_prefix.get() <= 8957081)
      //  return 25;
      //
      //if(DNA_prefix.get() > 8957081 && DNA_prefix.get() <= 9138125)
      //  return 26;
      //
      //if(DNA_prefix.get() > 9138125 && DNA_prefix.get() <= 9290364)
      //  return 27;
      //
      //if(DNA_prefix.get() > 9290364 && DNA_prefix.get() <= 9375000)
      //  return 28;
      //
      //if(DNA_prefix.get() > 9375000 && DNA_prefix.get() <= 9577731)
      //  return 29;
      //
      //if(DNA_prefix.get() > 9577731 && DNA_prefix.get() <= 9714122)
      //  return 30;
      //
      //return 31;

      //8 chars
      //if(DNA_prefix.get()<=99691)
      //  return 0;
      //else if(DNA_prefix.get()>99691 && DNA_prefix.get()<=104908)
      //  return 1;
      //else if(DNA_prefix.get()>104908 && DNA_prefix.get()<=109359)
      //  return 2;
      //else if(DNA_prefix.get()>109359 && DNA_prefix.get()<=116786)
      //  return 3;
      //else if(DNA_prefix.get()>116786 && DNA_prefix.get()<=128946)
      //  return 4;
      //else if(DNA_prefix.get()>128946 && DNA_prefix.get()<=134248)
      //  return 5;
      //else if(DNA_prefix.get()>134248 && DNA_prefix.get()<=140573)
      //  return 6;
      //else if(DNA_prefix.get()>140573 && DNA_prefix.get()<=149818)
      //  return 7;
      //else if(DNA_prefix.get()>149818 && DNA_prefix.get()<=154996)
      //  return 8;
      //else if(DNA_prefix.get()>154996 && DNA_prefix.get()<=177724)
      //  return 9;
      //else if(DNA_prefix.get()>177724 && DNA_prefix.get()<=182963)
      //  return 10;
      //else if(DNA_prefix.get()>182963 && DNA_prefix.get()<=187484)
      //  return 11;
      //else if(DNA_prefix.get()>187484 && DNA_prefix.get()<=201046)
      //  return 12;
      //else if(DNA_prefix.get()>201046 && DNA_prefix.get()<=222723)
      //  return 13;
      //else if(DNA_prefix.get()>222723 && DNA_prefix.get()<=229589)
      //  return 14;
      //else if(DNA_prefix.get()>229589 && DNA_prefix.get()<=234348)
      //  return 15;
      //else if(DNA_prefix.get()>234348 && DNA_prefix.get()<=260183)
      //  return 16;
      //else if(DNA_prefix.get()>260183 && DNA_prefix.get()<=270159)
      //  return 17;
      //else if(DNA_prefix.get()>270159 && DNA_prefix.get()<=280216)
      //  return 18;
      //else if(DNA_prefix.get()>280216 && DNA_prefix.get()<=291671)
      //  return 19;
      //else if(DNA_prefix.get()>291671 && DNA_prefix.get()<=303109)
      //  return 20;
      //else if(DNA_prefix.get()>303109 && DNA_prefix.get()<=310166)
      //  return 21;
      //else if(DNA_prefix.get()>310166 && DNA_prefix.get()<=333041)
      //  return 22;
      //else if(DNA_prefix.get()>333041 && DNA_prefix.get()<=340548)
      //  return 23;
      //else if(DNA_prefix.get()>340548 && DNA_prefix.get()<=349033)
      //  return 24;
      //else if(DNA_prefix.get()>349033 && DNA_prefix.get()<=357786)
      //  return 25;
      //else if(DNA_prefix.get()>357786 && DNA_prefix.get()<=365219)
      //  return 26;
      //else if(DNA_prefix.get()>365219 && DNA_prefix.get()<=371484)
      //  return 27;
      //else if(DNA_prefix.get()>371484 && DNA_prefix.get()<=375000)
      //  return 28;
      //else if(DNA_prefix.get()>375000 && DNA_prefix.get()<=383097)
      //  return 29;
      //else if(DNA_prefix.get()>383097 && DNA_prefix.get()<=388542)
      //  return 30;
      //else
      //  return 31;


      //if(DNA_prefix.get()<=797)
      //  return 0;
      //else if(DNA_prefix.get()>797 && DNA_prefix.get()<=839)
      //  return 1;
      //else if(DNA_prefix.get()>839 && DNA_prefix.get()<=874)
      //  return 2;
      //else if(DNA_prefix.get()>874 && DNA_prefix.get()<=934)
      //  return 3;
      //else if(DNA_prefix.get()>934 && DNA_prefix.get()<=1031)
      //  return 4;
      //else if(DNA_prefix.get()>1031 && DNA_prefix.get()<=1073)
      //  return 5;
      //else if(DNA_prefix.get()>1073 && DNA_prefix.get()<=1124)
      //  return 6;
      //else if(DNA_prefix.get()>1124 && DNA_prefix.get()<=1198)
      //  return 7;
      //else if(DNA_prefix.get()>1198 && DNA_prefix.get()<=1239)
      //  return 8;
      //else if(DNA_prefix.get()>1239 && DNA_prefix.get()<=1421)
      //  return 9;
      //else if(DNA_prefix.get()>1421 && DNA_prefix.get()<=1463)
      //  return 10;
      //else if(DNA_prefix.get()>1463 && DNA_prefix.get()<=1499)
      //  return 11;
      //else if(DNA_prefix.get()>1499 && DNA_prefix.get()<=1608)
      //  return 12;
      //else if(DNA_prefix.get()>1608 && DNA_prefix.get()<=1781)
      //  return 13;
      //else if(DNA_prefix.get()>1781 && DNA_prefix.get()<=1836)
      //  return 14;
      //else if(DNA_prefix.get()>1836 && DNA_prefix.get()<=1874)
      //  return 15;
      //else if(DNA_prefix.get()>1874 && DNA_prefix.get()<=2081)
      //  return 16;
      //else if(DNA_prefix.get()>2081 && DNA_prefix.get()<=2161)
      //  return 17;
      //else if(DNA_prefix.get()>2161 && DNA_prefix.get()<=2241)
      //  return 18;
      //else if(DNA_prefix.get()>2241 && DNA_prefix.get()<=2333)
      //  return 19;
      //else if(DNA_prefix.get()>2333 && DNA_prefix.get()<=2424)
      //  return 20;
      //else if(DNA_prefix.get()>2424 && DNA_prefix.get()<=2481)
      //  return 21;
      //else if(DNA_prefix.get()>2481 && DNA_prefix.get()<=2664)
      //  return 22;
      //else if(DNA_prefix.get()>2664 && DNA_prefix.get()<=2724)
      //  return 23;
      //else if(DNA_prefix.get()>2724 && DNA_prefix.get()<=2792)
      //  return 24;
      //else if(DNA_prefix.get()>2792 && DNA_prefix.get()<=2862)
      //  return 25;
      //else if(DNA_prefix.get()>2862 && DNA_prefix.get()<=2921)
      //  return 26;
      //else if(DNA_prefix.get()>2921 && DNA_prefix.get()<=2971)
      //  return 27;
      //else if(DNA_prefix.get()>2971 && DNA_prefix.get()<=3000)
      //  return 28;
      //else if(DNA_prefix.get()>3000 && DNA_prefix.get()<=3064)
      //  return 29;
      //else if(DNA_prefix.get()>3064 && DNA_prefix.get()<=3108)
      //  return 30;
      //else
      //  return 31;
    
      //char 5
      //if(DNA_prefix.get() <= 796)
      //  return 0;
      //
      //if(DNA_prefix.get() > 796 && DNA_prefix.get() <= 838)
      //  return 1;
      //
      //if(DNA_prefix.get() > 838 && DNA_prefix.get() <= 875)
      //  return 2;
      //
      //if(DNA_prefix.get() > 875 && DNA_prefix.get() <= 936)
      //  return 3;
      //
      //if(DNA_prefix.get() > 936 && DNA_prefix.get() <= 1033)
      //  return 4;
      //
      //if(DNA_prefix.get() > 1033 && DNA_prefix.get() <= 1081)
      //  return 5;
      //
      //if(DNA_prefix.get() > 1081 && DNA_prefix.get() <= 1156)
      //  return 6;
      //
      //if(DNA_prefix.get() > 1156 && DNA_prefix.get() <= 1199)
      //  return 7;
      //
      //if(DNA_prefix.get() > 1199 && DNA_prefix.get() <= 1242)
      //  return 8;
      //
      //if(DNA_prefix.get() > 1242 && DNA_prefix.get() <= 1422)
      //  return 9;
      //
      //if(DNA_prefix.get() > 1422 && DNA_prefix.get() <= 1466)
      //  return 10;
      //
      //if(DNA_prefix.get() > 1466 && DNA_prefix.get() <= 1531)
      //  return 11;
      //
      //if(DNA_prefix.get() > 1531 && DNA_prefix.get() <= 1612)
      //  return 12;
      //
      //if(DNA_prefix.get() > 1612 && DNA_prefix.get() <= 1786)
      //  return 13;
      //
      //if(DNA_prefix.get() > 1786 && DNA_prefix.get() <= 1839)
      //  return 14;
      //
      //if(DNA_prefix.get() > 1839 && DNA_prefix.get() <= 2031)
      //  return 15;
      //
      //if(DNA_prefix.get() > 2031 && DNA_prefix.get() <= 2084)
      //  return 16;
      //
      //if(DNA_prefix.get() > 2084 && DNA_prefix.get() <= 2164)
      //  return 17;
      //
      //if(DNA_prefix.get() > 2164 && DNA_prefix.get() <= 2243)
      //  return 18;
      //
      //if(DNA_prefix.get() > 2243 && DNA_prefix.get() <= 2334)
      //  return 19;
      //
      //if(DNA_prefix.get() > 2334 && DNA_prefix.get() <= 2432)
      //  return 20;
      //
      //if(DNA_prefix.get() > 2432 && DNA_prefix.get() <= 2483)
      //  return 21;
      //
      //if(DNA_prefix.get() > 2483 && DNA_prefix.get() <= 2669)
      //  return 22;
      //
      //if(DNA_prefix.get() > 2669 && DNA_prefix.get() <= 2733)
      //  return 23;
      //
      //if(DNA_prefix.get() > 2733 && DNA_prefix.get() <= 2794)
      //  return 24;
      //
      //if(DNA_prefix.get() > 2794 && DNA_prefix.get() <= 2866)
      //  return 25;
      //
      //if(DNA_prefix.get() > 2866 && DNA_prefix.get() <= 2924)
      //  return 26;
      //
      //if(DNA_prefix.get() > 2924 && DNA_prefix.get() <= 2972)
      //  return 27;
      //
      //if(DNA_prefix.get() > 2972 && DNA_prefix.get() <= 3000)
      //  return 28;
      //
      //if(DNA_prefix.get() > 3000 && DNA_prefix.get() <= 3064)
      //  return 29;
      //
      //if(DNA_prefix.get() > 3064 && DNA_prefix.get() <= 3108)
      //  return 30;
      //
      //return 31;
  }
}

