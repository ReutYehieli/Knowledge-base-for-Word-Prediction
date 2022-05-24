
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class WorkJobs {
    public static void main(String[] args){
        try{
            // create the hash map for the stop words:
            HashSet <String> stopWords = new HashSet<>();
            // args[0] = the file of stopwords

            File stopWordsFile = new File(args[0]);
            Scanner myReader = new Scanner(stopWordsFile);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                stopWords.add(data);
            }
            myReader.close();



            /////// first job///////
            Configuration conf1 = new Configuration();
            conf1.set("IsWordStop","stopWord"); ///////////////////////////////
            Job job = new Job(conf1,"firstJob");
            job.setJarByClass(WordCount.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(WordCount.Reduce.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setNumReduceTasks(1);


        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
