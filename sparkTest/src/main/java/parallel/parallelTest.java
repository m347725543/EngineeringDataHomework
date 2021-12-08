package parallel;

import util.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.File;
import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 2021.11.30
 * maybe after reading all data and map lines to words collection
 * use filter() with hash() distribute words to R regions (RDD<String)
 * so we can assign data to mutil-threads
 */

public class parallelTest{

    public static void main(String[] args) {
        // delete previous output file
        File output = new File("/usr/local/data1");
        FileUtil.DeleteFile(output);
        // create thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        //configure spark conf , local mean spark run in local
        SparkConf sparkConf = new SparkConf().setAppName("sparkBoot");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // since spark lazy execute, so if you need run muti threads at the same time
        // you need read data to cache before execute handling jobs
        // if /data is a direction , all file in it will be loaded
        JavaRDD<String> lines = sparkContext.textFile("/mnt/hgfs/vm_share/wikipedia-dump.txt").cache();
//        JavaRDD<String> lines = sparkContext.textFile("/usr/local/training.1600000.processed.noemoticon.txt").cache();
        // data form is lines , split this collection to M piece ,assign them to each thread
//        lines.map(new Function<String, Object>() {
//            @Override
//            public Object call(String s) {
//                return s;
//            }
//        });

        // map lines to words form
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) {
//                return Arrays.asList(s.split("[^a-zA-Z]+")).iterator();
//            }
//        });
//        // map words to struct KV
//        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) {
//                return new Tuple2<String, Integer>(s, 1);
//            }
//        });
        // thread pool assign thread execute reduce task
        for (int i = 0 ; i < 1; i++){
            executorService.execute(new Task(lines, i));
        }
//        executorService.execute(new Task(wordsOnes));
        // this has cause an error task is not serializable
        // maybe cause by the reduceByKey use outside param wordsOnes
//        executorService.execute(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("----------Thread start------------");
//                JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer value, Integer toValue) {
//                        return value + toValue;
//                    }
//                });//JavaPari
//                wordsCounts.saveAsTextFile("/usr/local/data1");
//                System.out.println("----------Thread end------------");
//            }
//        });
        //this only reject new task
        executorService.shutdown();
        // wait all task complete
        boolean res = true;
        while (res){
            try {
                res = !executorService.awaitTermination(10, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                // none action
            }
        }

        while (true);
    }//main

    static class Task implements Runnable, Serializable{
        JavaPairRDD<String, Integer> wordsOnes;
        JavaRDD<String> words;
        JavaRDD<String> lines;
        int index;

        public Task(JavaPairRDD<String, Integer> wordsOnes) {
            this.wordsOnes = wordsOnes;
        }

//        public Task(JavaRDD<String> words, int index){
//            this.words = words;
//            this.index = index;
//        }

        public Task(JavaRDD<String> lines, int index){
            this.lines = lines;
            this.index = index;
        }

        @Override
        public void run() {
            System.out.println("----------Task start------------");
            // map words to struct KV
//            JavaRDD<String> filterWords = words.filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String s) throws Exception {
//                    if(s.hashCode() % 3 == index)
//                        return true;
//                    return false;
//                }
//            });
            lines.map(new Function<String, Object>() {
                @Override
                public Object call(String s) {
                    return s;
                }
            });

            JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String s) {
                    return Arrays.asList(s.split("[^a-zA-Z]+")).iterator();
                }
            });

            JavaPairRDD<String, Integer> wordsOnes = words.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) {
                    return new Tuple2<String, Integer>(s, 1);
                }
            }).cache();

            JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer value, Integer toValue) {
                    return value + toValue;
                }
            });//JavaPari

            wordsCounts.saveAsTextFile("/usr/local/data1/" + index);

            System.out.println("----------Task end------------");
        }//run()
    }//Task
}
