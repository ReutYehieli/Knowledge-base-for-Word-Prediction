import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FirstJob {
    /// The first job will get one-garms dataset. and will count N & Ci for each wi
    public static class MapperClass extends Mapper<LongWritable, Text, Text, CorpusOccurrences>  {
        //private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line.length == 5) {
                if (!IsStopWords.containsKey(line[0])) {
                    int year = Integer.parseInt(line[1]);
                    int decade;
                    if (year >= 1990 && year <= 1999) {
                        decade = 1;
                    } else if (year >= 2000 && year <= 2009) {
                        decade = 2;
                    } else decade = 3;
                    context.write(new Text(line[0]), new CorpusOccurrences(decade, Integer.parseInt(line[2])));
                }
            }
            else { System.out.println("problem in the mapper of FirstJob - incorrect number of words");}
    }
    }

}
