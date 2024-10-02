package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class App2 {
    public static void main(String[] args){
        SparkConf conf =new SparkConf().setAppName("TP 1").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> nombres= Arrays.asList(new Integer[]{12,10,16,9,8,0});
        JavaRDD<String> rddLines=sc.textFile("words.txt");
        JavaRDD<String> rddWords=rddLines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordsPair=rddWords.mapToPair(word->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordCount=wordsPair.reduceByKey((a,b)->a+b);
        wordCount.foreach(e-> System.out.println(e._1()+" "+e._2()));

    }
}
