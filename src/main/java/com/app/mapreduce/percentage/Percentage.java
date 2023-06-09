package com.app.mapreduce.percentage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.util.UUID;

public class Percentage {

	public static void main(String[] args) throws Exception {
		String log4jConfPath = "src/main/resources/application.properties";
		PropertyConfigurator.configure(log4jConfPath);

		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (pathArgs.length < 2) {
			System.err.println("Usage: percentage-map-reduce <input-path> [...] <output-path>");
			System.exit(2);
		}
		Job wcJob = Job.getInstance(conf, "MapReduce Percentage");
		wcJob.setJarByClass(Percentage.class);
		wcJob.setMapperClass(PercentageMapper.class);
		wcJob.setCombinerClass(PercentageReducer.class);
		wcJob.setReducerClass(PercentageReducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < pathArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(wcJob, new Path(pathArgs[i]));
		}
		        FileOutputFormat.setOutputPath(wcJob, new Path(pathArgs[pathArgs.length - 1] + UUID.randomUUID()));
		System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
	}

}