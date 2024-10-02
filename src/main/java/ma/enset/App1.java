package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class App1 {
    public static void main(String[] args){
        SparkConf conf =new SparkConf().setAppName("TP 1").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> nombres= Arrays.asList(new Integer[]{12,10,16,9,8,0});
        JavaRDD<Integer> rdd1=sc.parallelize(nombres);
        JavaRDD<Integer> rdd2=rdd1.map(e->e+1);
        JavaRDD<Integer> rdd3=rdd2.filter(e->e>=10);
        rdd3.foreach(e->System.out.println(e));

    }
}
