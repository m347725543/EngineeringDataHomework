import util.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.io.File;
import java.util.*;

/**
 * WordCount
 *
 * input data and result save in share directory
 */
public class test {
    public static void main(String[] args) throws InterruptedException {
        // delete previous result
        File output = new File("/mnt/hgfs/vm_share/result");
        FileUtil.DeleteFile(output);

        // spark conf
        SparkConf sparkConf = new SparkConf().setAppName("sparkBoot");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> lines = sparkContext.textFile("/mnt/hgfs/vm_share/wikipedia-dump.txt").cache();
        JavaRDD<String> lines = sparkContext.textFile("/usr/local/data").cache();

        // map lines
        lines.map((Function<String, Object>) s -> s);
        // map lines to word form
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>)
                s -> Arrays.asList(s.split("[^a-zA-Z]+")).iterator());
        // map words to key-value form
        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair((PairFunction<String, String, Integer>)
                s -> new Tuple2<String, Integer>(s, 1));
        // reduce KV
        JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(
                (Function2<Integer, Integer, Integer>) (value, toValue) -> value + toValue);
        // save result
        wordsCounts.saveAsTextFile("/mnt/hgfs/vm_share/result/");

//        while (true)
//            Thread.sleep(10000);
    }

}
