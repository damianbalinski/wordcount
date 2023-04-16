package edu.ucr.cs.merlin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	private IntWritable count = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		// gurukul [1 1 1 1 1 1....]
		int valueSum = 0;
		for (IntWritable val : values)
		{
			valueSum += val.get();
		}
		count.set(valueSum);
		context.write(key, count);
	}
}