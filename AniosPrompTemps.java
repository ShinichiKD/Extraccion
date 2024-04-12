import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AniosPrompTemps {

    public static class TempMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text Anio = new Text();
        private FloatWritable temperatura = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Suponemos que el formato de entrada es "Ciudad Fecha TempMin TempMax"
            String[] parts = value.toString().split(" ");
            if (parts.length == 4) {
                try {
                    String dateStr = parts[1];
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(df.parse(dateStr));
                    int year = cal.get(Calendar.YEAR);
                    Anio.set(Integer.toString(year));

                    float tempMin = Float.parseFloat(parts[2]);
                    float tempMax = Float.parseFloat(parts[3]);
                    float avgTemp = (tempMin + tempMax) / 2.0f;  
                    temperatura.set(avgTemp);
                    context.write(Anio, temperatura);
                } catch (Exception e) {
                    
                }
            }
        }
    }

    public static class AvgTempReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                float average = sum / count;
                
                String formattedAverage = String.format("%.1f", average).replace('.', ',');
                result.set(formattedAverage);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperaturas promedio por año");
        job.setJarByClass(AniosPrompTemps.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(AvgTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
