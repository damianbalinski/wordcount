package com.app.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CSVPrettifier {

	private final static int NUMBER_OF_COLUMNS = 24;
	private final static char NEW_LINE = '\n';
	private final static char SPACE = ' ';
	private final static char DELIMITER = ',';
	private final static char DOUBLE_QUOTE = '"';

	public static void main(String[] args) throws IOException {
		String inputFile = "./download/Iowa_Liquor_Sales.csv";
		String outputFile = "./download/Iowa_Liquor_Sales_pretty.csv";

		try (BufferedReader br = new BufferedReader(new FileReader(inputFile));
				FileWriter fw = new FileWriter(outputFile)) {

			String line;
			boolean inString = false;
			int counter = 0;
			StringBuilder currentOutput = new StringBuilder();

			br.readLine();
			while ((line = br.readLine()) != null) {
				currentOutput.append(line);
				for (char c: line.toCharArray()) {
					if (c == DOUBLE_QUOTE) {
						inString = !inString;
					}
					if (c == DELIMITER) {
						counter += inString ? 0 : 1;
					}
				}
				if (counter == NUMBER_OF_COLUMNS-1) {
					fw.write(currentOutput.toString().replace(NEW_LINE, SPACE) + "\n");
					currentOutput.setLength(0);
					counter = 0;
				}
			}
			fw.flush();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
