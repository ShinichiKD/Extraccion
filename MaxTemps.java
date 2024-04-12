import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemps {

  public static class TempMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text city = new Text();
    private IntWritable tempMax = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] parts = value.toString().split(" ");
      if (parts.length == 4) {
        ciudad.set(parts[0]);  
        tempMax.set(Integer.parseInt(parts[3])); 
        context.write(ciudad, tempMax);
      }
    }
  }

  public static class MaxTempReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int maxTemp = Integer.MIN_VALUE;
      for (IntWritable val : values) {
        if (val.get() > maxTemp) {
          maxTemp = val.get();
        }
      }
      result.set(maxTemp);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Temperaturas maximas");
    job.setJarByClass(MaxTemps.class);
    job.setMapperClass(TempMapper.class);
    job.setReducerClass(MaxTempReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
