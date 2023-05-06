package com.app.mapreduce.percentage;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

class PercentageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static Splitter CSV_SPLITTER = Splitter.on(Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"));

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0L) {
			return;
		}
		List<String> tokens = CSV_SPLITTER.splitToList(value.toString());
		context.write(new Text(tokens.get(11)), new IntWritable(1));
	}

}