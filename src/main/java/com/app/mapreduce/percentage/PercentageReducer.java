package com.app.mapreduce.percentage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class PercentageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int counter = 0;
		for (IntWritable val : values) {
			counter += val.get();
		}
		context.write(key, new IntWritable(counter));
	}

}