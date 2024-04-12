import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;

public class PromTemps {

    public static class TempMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text ciudad = new Text();
        private IntWritable tempValue = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            if (parts.length == 4) {
                ciudad.set(parts[0]);  
                int tempMin = Integer.parseInt(parts[2]);
                int tempMax = Integer.parseInt(parts[3]);
                int averageTemp = (tempMin + tempMax) / 2;  
                tempValue.set(averageTemp);
                context.write(ciudad, tempValue);
            }
        }
    }

    public static class AvgTempReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            float average = sum / count;  
            result.set(average);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperatura Promedio");
        job.setJarByClass(PromTemps.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(AvgTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
