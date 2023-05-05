package com.app.mapreduce.alcohol;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AlcoholReducer extends Reducer<Text, Text, Text, Text>
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (final Text value : values) {
			context.write(key, value);
		}
	}
}