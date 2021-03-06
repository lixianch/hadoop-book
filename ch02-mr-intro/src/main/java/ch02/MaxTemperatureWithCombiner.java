package ch02;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lixianch on 2016/8/4.
 */
public class MaxTemperatureWithCombiner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2){
            System.out.println("Usage: MaxTemperatureWithCombiner <input path> " +
                    "<output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperatureWithCombiner.class);
        job.setJobName("Max temperature with combiner");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
