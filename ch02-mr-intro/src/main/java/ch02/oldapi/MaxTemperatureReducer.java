package ch02.oldapi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by lixianch on 2016/8/9.
 */
public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text,IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int maxTemperature = Integer.MIN_VALUE;
        while(iterator.hasNext()){
            maxTemperature = Math.max(maxTemperature, iterator.next().get());
        }

        outputCollector.collect(text, new IntWritable(maxTemperature));
    }
}
