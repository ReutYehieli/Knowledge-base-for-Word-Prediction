import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//<w1w2Decade,value>
//<w1decase, value>
public class JoinAllDetails {

    public static class MapperClass extends Mapper<LongWritable, Text, KeyForFirstJoin, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] args = line.toString().split("\t");
            if (args[1].equals("*")) {  
                context.write(new KeyForFirstJoin(args[2],args[0],"a"), new Text("from1gram" + "\t" + args[4]);
            } else { 
                context.write( new KeyForFirstJoin(args[2],args[1],"b"), new Text("from2gram" + "\t" + args[0]+"\t"+ args[3]+"\t"+args[4]+"\t"+args[5]));
        
            }
        }
    }

    public static class ReducerClass extends Reducer<KeyForFirstJoin, Text, Text, Text> {
        String t;
        String numberOfOccW2;

        public void setup(Context context) {
            t="";
            numberOfOccW2 = "0";
        }

        public void reduce(KeyForFirstJoin key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                String[] args = value.toString().split("\t");
                System.out.println(args[0]);
                if(args[0].equals("from1gram")){
                    numberOfOccW2 = args[1];
                }
                else if(args[0].equals("from2gram")){
                    String[] keyString = key.toString().split("\t");
                  //  <w1,w2,decade>
                    double c1 = Double.parseDouble(args[2]);
                    double c2 = Double.parseDouble(numberOfOccW2); 
                    double c12 = Double.parseDouble(args[3]);
                    double N = Double.parseDouble(args[4]);
                    if(N != 0) p = c2/N ;
                    if( c1!=0 ) p1 =  c12/c1;
                    double p2 = (c2 -c12)/2;
                    if(N-c1 != 0){p2 = (c2 -c12)/ (N -c1);
                    double cal = (Math.log(LFunction(c12, c1, p)) + Math.log(LFunction(c2 - c12,N - c1,p)) - Math.log(LFunction(c12, c1, p1)) - Math.log(LFunction(c2 - c12,N - c1,p2)))*(-2.0);
                    context.write(new Text(args[1]+"\t"+keyString[1]+"\t"+keyString[0]+"\t"+cal), new Text(""));
                    }
                else{
                    System.out.println("There was a problem with the First Join - in Reduce");
                }
            }
        }

        public void cleanup(Context context) {
        }

    }
    public static double LFunction(double k , double n, double x){
        return  Math.pow(x, k) * Math.pow((1.0 - x) , (n -k )) ;
    }

    public static class PartitionerClass extends Partitioner<KeyForFirstJoin, Text> {
        @Override
        public int getPartition(KeyForFirstJoin key, Text text, int numPartitions) {
            return key.tmphashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("starting job 4");
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4,"second join");
        job4.setJarByClass(JoinAllDetails.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(KeyForFirstJoin.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setMapperClass(JoinAllDetails.MapperClass.class);
        job4.setReducerClass(JoinAllDetails.ReducerClass.class);
        //job3.setCombinerClass(ThirdJob.CombinerClass.class);
        job4.setPartitionerClass(JoinAllDetails.PartitionerClass.class);
        //  job3.setNumReduceTasks(32);

        //job4.setInputFormatClass(TextInputFormat.class);


        MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket1/output2"),TextInputFormat.class); // path need to be with one grams.
        MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket1/output4"),TextInputFormat.class);
        FileOutputFormat.setOutputPath(job4,new Path("s3://ass2bucket1/output5"));  //"s3://ass2bucket2/output5/" change
         job4.setOutputFormatClass(TextOutputFormat.class);
        job4.waitForCompletion(true);
          if (job4.isSuccessful()){
             System.out.println("Finish the second job");
            }
          else {
          throw new RuntimeException("Job failed : " + job4);
         }
    }
}
