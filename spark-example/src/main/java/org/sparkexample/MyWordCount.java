package org.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

@SuppressWarnings("serial")
public class MyWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCounter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/mvpzhao/Tools/java-workspace/springboot-study/spark-example/src/main/java/org/sparkexample/all_log.log", 1);

        // 切分为单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            // 以前的版本好像是Iterable而不是Iterator
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // 转换为键值对并计数
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer e, Integer acc) throws Exception {
                return e + acc;
            }
        }, 1);

        result.map(new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2(v1._1, v1._2);
            }
        }).sortBy(new Function<Tuple2<String, Integer>, Integer>() {
            public Integer call(Tuple2<String, Integer> v1) throws Exception {
                return v1._2;
            }
        }, false, 1).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> e) throws Exception {
                System.out.println("【" + e._1 + "】出现了" + e._2 + "次");
            }
        });

        sc.close();

    }
}
