package examples;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

public class WordCountMapper implements Mapper<LongWritable, Text, Text, LongWritable> {

    public void configure(JobConf jobConf) {
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter)
            throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken();
    	collector.collect(new Text(word), new LongWritable(1));
      }
      // collector.collect(value, key);
    }

    public void close() throws IOException {
    }

}
