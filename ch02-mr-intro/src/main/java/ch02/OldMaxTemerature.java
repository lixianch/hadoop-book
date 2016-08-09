package ch02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by lixianch on 2016/8/9.
 */
public class OldMaxTemerature {
    static class OldMaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text,Text, IntWritable>{
        private static final int MISSING = 9999;
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String year = line.substring(15, 19);
            int airTemperature;
            if(line.charAt(87) == '+'){
                airTemperature = Integer.parseInt(line.substring(88, 92));
            }{
                airTemperature = Integer.parseInt(line.substring(87, 92));
            }
            String quantity = line.substring(92, 93);
            if(quantity.matches("[01459]") && airTemperature != MISSING){
                outputCollector.collect(new Text(year), new IntWritable(airTemperature));
            }
        }
    }

    static class OldMaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int maxTemperature = Integer.MIN_VALUE;
            while(iterator.hasNext()){
                maxTemperature = Math.max(maxTemperature, iterator.next().get());
            }
            outputCollector.collect(text, new IntWritable(maxTemperature));
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 2){
            System.err.println("Usage: OldMaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        JobConf jobConf = new JobConf(OldMaxTemerature.class);
        jobConf.setJobName("Old Max Temperature");

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        jobConf.setMapperClass(OldMaxTemperatureMapper.class);
        jobConf.setReducerClass(OldMaxTemperatureReducer.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(jobConf);
    }
}
