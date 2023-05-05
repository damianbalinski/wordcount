package com.app.mapreduce.alcohol;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class AlcoholMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private final static String DELIMITER = ",";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		if (key.get() == 0L) {
			return;
		}
		String line = value.toString();
		String[] row = line.split(DELIMITER);
		context.write(extractKey(row), extractValue(row));
	}

	private Text extractKey(String[] row) {
		return new Text(row[0]);
	}

	private Text extractValue(String[] row) {
		return new Text(String.join(",", new String[]{
				row[1],  // date
				row[5],  // city
				"temp",  // type
				row[21], // price
				row[22], // volume sold
				row[3],  // store name
		}));
	}
}