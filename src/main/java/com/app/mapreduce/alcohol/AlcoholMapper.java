package com.app.mapreduce.alcohol;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

class AlcoholMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static Splitter CSV_SPLITTER = Splitter.on(Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"));
	private final static String PERCENTAGE_FILE_NAME = "src/main/resources/percentage.csv";
	private final static Map<String, Integer> PERCENTAGE = importPercentage();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0L) {
			return;
		}
		List<String> tokens = CSV_SPLITTER.splitToList(value.toString());
		if (!containsNecessaryFields(tokens)) {
			return;
		}
		context.write(extractKey(tokens), extractValue(tokens));
	}

	private Text extractKey(List<String> tokens) {
		return new Text(tokens.get(0));
	}

	private Text extractValue(List<String> tokens) {
		Integer percentage = PERCENTAGE.get(tokens.get(11));
		Double volumeSold = Double.valueOf(tokens.get(22));
		Double pureAlcohol = volumeSold * percentage;
		return new Text(String.join(",", new String[]{
				tokens.get(1),  // date
				tokens.get(5),  // city
				tokens.get(21), // price
				String.valueOf(pureAlcohol),
				tokens.get(3)	// store name
		}));
	}

	private boolean containsNecessaryFields(List<String> tokens) {
		String categoryName = tokens.get(11);
		String volumeSold = tokens.get(22);
		return volumeSold != null
				&& categoryName != null
				&& PERCENTAGE.containsKey(categoryName);
	}

	private static Map<String, Integer> importPercentage() {
		Map<String, Integer> percentage = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(PERCENTAGE_FILE_NAME))) {
			String line;
			while ((line = br.readLine()) != null) {
				List<String> tokens = CSV_SPLITTER.splitToList(line);
				percentage.put(tokens.get(0), Integer.valueOf(tokens.get(1)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return percentage;
	}

}