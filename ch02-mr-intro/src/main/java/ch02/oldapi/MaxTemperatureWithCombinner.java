package ch02.oldapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Created by lixianch on 2016/8/9.
 */
public class MaxTemperatureWithCombinner {
    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf(MaxTemperatureWithCombinner.class);
        jobConf.setJobName("Old Max Temperature With Combinner");

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        jobConf.setMapperClass(MaxTemperatureMapper.class);
        jobConf.setCombinerClass(MaxTemperatureReducer.class);
        jobConf.setReducerClass(MaxTemperatureReducer.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(jobConf);
    }
}
