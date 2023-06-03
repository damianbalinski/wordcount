package com.app.mapreduce.alcohol;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AlcoholReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (final Text value : values) {
			context.write(NullWritable.get(), value);
		}
	}
}
