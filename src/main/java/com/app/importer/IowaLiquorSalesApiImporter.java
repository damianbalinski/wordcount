package com.app.importer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;

import static java.text.MessageFormat.format;

@Slf4j
public class IowaLiquorSalesApiImporter {

	public static void main(String[] args) {
		String log4jConfPath = "src/main/resources/application.properties";
		PropertyConfigurator.configure(log4jConfPath);

		if (args.length != 2) {
			System.out.println("Invalid usage, try IowaLiquorSalesApiImporter.java <YYYY-MM-dd> <YYYY-MM-dd>");
		}
		String startDate = args[0];
		String endDate = args[1];

		String downloadDirectory = format("{0}/src/main/resources/", System.getProperty("user.dir"));
		String hadoopDirectory = "/user/hduser/iowa/";
		String fileName = format("Iowa_Liquor_Sales_from_{0}_to_{1}.csv", startDate, endDate);
		String fileUrl = "https://data.iowa.gov/resource/m3tr-qhgy.csv";

		try {
			Instant start = Instant.now();
			log.info(format("Begin process of data acquisition"));

			log.info("Trying to download file, fileUrl = {}", fileUrl);
			Duration downloadTime = downloadFile(fileUrl, startDate, endDate, downloadDirectory, fileName);
			log.info("File downloaded successfully, fileName = {}", fileName);
			log.info("Download time = {} ms", downloadTime.toMillis());

			log.info("Trying to copy file into container");
			Duration copyTime = copyFileIntoContainer(downloadDirectory, fileName);
			log.info("File copied into container successfully");
			log.info("Copy file = {} ms", copyTime.toMillis());

			log.info("Trying to put file into hadoop storage, hadoopDirectory = {}", hadoopDirectory);
			Duration putTime = putFileIntoHadoop(fileName, hadoopDirectory);
			log.info("Put file into hadoop storage successfully");
			log.info("Put time = {} ms", putTime.toMillis());

			log.info("Trying to replicate file on hadoop storage");
			Duration replicationTime = replicateFile(fileName, hadoopDirectory);
			log.info("File replicated successfully");
			log.info("Replication time = {} ms", replicationTime.toMillis());

			log.info("End process of data acquisition");
			Instant end = Instant.now();

			log.info("Total time of data acquisition process = {} ms", Duration.between(start, end).toMillis());

		} catch (Exception e) {
			log.error("Exception occurred during data acquisition process", e);
			e.printStackTrace();
		}
	}

	private static Duration replicateFile(String fileName, String hadoopDirectory) throws Exception {
		Instant start = Instant.now();
		String filePath = hadoopDirectory + fileName;

		String cmd = format("docker exec master hdfs dfs -setrep -w 3 {0}", filePath);
		Process process = Runtime.getRuntime().exec(cmd);
		InputStream inputStream = process.getInputStream();
		int replicationExitCode = process.waitFor();
		String input = new String(IOUtils.toByteArray(inputStream));
		log.info("Hadoop replication exitCode = {}, output = {}", replicationExitCode, input);

		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration copyFileIntoContainer(String fileDirectory, String fileName) throws Exception {
		Instant start = Instant.now();
		String filePath = fileDirectory + fileName;
		String dockerPath = format("/tmp/{0}", fileName);
		String cmdCopyFile = format("docker cp {0} master:{1}", filePath, dockerPath);

		int copyFileExitCode = Runtime.getRuntime().exec(cmdCopyFile).waitFor();
		log.info("Container copy file exitCode = {}", copyFileExitCode);

		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration putFileIntoHadoop(String fileName, String hadoopDirectory) throws Exception {
		// hadoop fs -put ./book_levels.csv /user/hduser/input
		Instant start = Instant.now();
		String filePath = "/tmp/" + fileName;

		String cmdCreateDir = format("docker exec master hadoop fs -mkdir -p {0}", hadoopDirectory);
		Process createDirProcess = Runtime.getRuntime().exec(cmdCreateDir);
		InputStream createDirInput = createDirProcess.getInputStream();
		int createDirExitCode = createDirProcess.waitFor();
		log.info("Hadoop create directory exitCode = {}, output = {}", createDirExitCode, new String(IOUtils.toByteArray(createDirInput)));

		String cmdPutFile = format("docker exec master hadoop fs -put {0} {1}", filePath, hadoopDirectory);
		Process putFileProcess = Runtime.getRuntime().exec(cmdPutFile);
		InputStream putFileInput = putFileProcess.getInputStream();
		int putFileExitCode = putFileProcess.waitFor();
		log.info("Hadoop put file exitCode = {}, output = {}", putFileExitCode, new String(IOUtils.toByteArray(putFileInput)));

		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration downloadFile(String fileUrl, String startDate, String endDate, String downloadDirectory, String fileName) throws IOException {
		Instant start = Instant.now();
		String fileUrlSize = format("https://data.iowa.gov/resource/m3tr-qhgy.json?$query=SELECT%20COUNT(*)%20WHERE%20date%20BETWEEN%20%22{0}%22%20AND%20%22{1}%22", startDate, endDate);
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = mapper.readValue(new URL(fileUrlSize).openStream(), JsonNode.class);
		String numberOfRows = jsonNode.get(0).get("COUNT").asText();
		String filePath = downloadDirectory + fileName;
		String fileUrlData = format("{0}?$query=SELECT%20*%20WHERE%20date%20BETWEEN%20%22{1}%22%20AND%20%22{2}%22%20LIMIT%20{3}", fileUrl, startDate, endDate, numberOfRows);
		try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrlData).openStream());
		     FileOutputStream fileOutputStream = new FileOutputStream(filePath)) {
			byte[] dataBuffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
				fileOutputStream.write(dataBuffer, 0, bytesRead);
			}
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

}
