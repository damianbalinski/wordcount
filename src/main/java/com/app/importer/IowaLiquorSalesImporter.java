package com.app.importer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;

import static java.text.MessageFormat.format;

@Slf4j
public class IowaLiquorSalesImporter {

	public static void main(String[] args) {
		String log4jConfPath = "src/main/resources/application.properties";
		PropertyConfigurator.configure(log4jConfPath);

		// if (args.length != 5) {
		// 		System.out.println("Invalid usage, try importer <downloadDirectory> <hadoopDirectory> <fileName> <fileUrl>");
		// }
		// String downloadDirectory = args[0];
		// String hadoopDirectory = args[1];
		// String downloadFileName = args[2];
		// String fileName = args[3];
		// String fileUrl = args[4];

		String downloadDirectory = "C:/src/java/wordcount/download/";
		String hadoopDirectory = "/user/hduser/iowa";
		String downloadFileName = "archive.zip";
		String fileName = "Iowa_Liquor_Sales.csv";
		String fileUrl = "https://storage.googleapis.com/kaggle-data-sets/4609/1138586/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com@kaggle-161607.iam.gserviceaccount.com/20230505/auto/storage/goog4_request&X-Goog-Date=20230505T092500Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=38a7f074755ef7bfa74e2cba787de82b5edc184be035f3b211b652879b71b36781e7702467eff32c342f6b11609167b7bf70453531e4cb1dd88e91e43870c3fcb316058f0f2ef77de3dbfa7d79452870abec1b3aa70f6003c38241b909b6db3368f58a8ce6382e6f134419cb2098e14413b085452e33f32a4b3fc824669fbafc6ce62fdc8097eb2c1ba43b66dfffebf3a0468f270320777017932a9fd39e93a656711980fe6418c2c4fb1bfd7b9f8008fbf69bdb70a8393b4588af8c7d3816359460adceada8c13ebb507a9ac6399a4156951bc70e26f4b512853eb8ce1a8db9fc4780a1118ced91a7cab976ab9aea531031e97a86aaca9e0935c02d1049e6d5";

		try {
			Instant start = Instant.now();
			log.info(format("Begin process of data acquisition"));

			log.info("Trying to download file, fileUrl = {}", fileUrl);
			Duration downloadTime = downloadFile(fileUrl, downloadDirectory, downloadFileName);
			log.info("File downloaded successfully, fileName = {}", downloadFileName);
			log.info("Download time = {} ms", downloadTime.toMillis());

			if (downloadFileName.endsWith(".zip")) {
				log.info("Trying to decompress zip archive, fileName = {}", downloadFileName);
				Duration unzipTime = unzipFile(downloadDirectory, downloadFileName, downloadDirectory);
				Long fileSize = getFileSize(downloadDirectory, fileName);
				log.info("File decompressed successfully, fileName = {}, fileSize = {} B", fileName, fileSize);
				log.info("Decompress time = {} ms", unzipTime.toMillis());
			}

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
		String input = new String(inputStream.readAllBytes());
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
		log.info("Hadoop create directory exitCode = {}, output = {}", createDirExitCode, new String(createDirInput.readAllBytes()));

		String cmdPutFile = format("docker exec master hadoop fs -put {0} {1}", filePath, hadoopDirectory);
		Process putFileProcess = Runtime.getRuntime().exec(cmdPutFile);
		InputStream putFileInput = putFileProcess.getInputStream();
		int putFileExitCode = putFileProcess.waitFor();
		log.info("Hadoop put file exitCode = {}, output = {}", putFileExitCode, new String(putFileInput.readAllBytes()));

		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration downloadFile(String fileUrl, String zipDirectory, String zipFileName) throws IOException {
		Instant start = Instant.now();
		String zipFilePath = zipDirectory + zipFileName;
		try (BufferedInputStream in = new BufferedInputStream(new URL(fileUrl).openStream());
				FileOutputStream fileOutputStream = new FileOutputStream(zipFilePath)) {
			byte dataBuffer[] = new byte[1024];
			int bytesRead;
			while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
				fileOutputStream.write(dataBuffer, 0, bytesRead);
			}
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration unzipFile(String zipDirectory, String zipFileName, String fileDirectory) throws IOException {
		Instant start = Instant.now();
		try {
			String zipFilePath = zipDirectory + zipFileName;
			FileInputStream fis = new FileInputStream(zipFilePath);
			ZipArchiveInputStream zis = new ZipArchiveInputStream(fis);

			ZipArchiveEntry entry = null;
			while ((entry = zis.getNextZipEntry()) != null) {
				String entryName = entry.getName();
				File entryFile = new File(fileDirectory, entryName);

				if (entry.isDirectory()) {
					entryFile.mkdirs();
				} else {
					byte[] buffer = new byte[1024];
					int length;
					FileOutputStream fos = new FileOutputStream(entryFile);
					while ((length = zis.read(buffer)) > 0) {
						fos.write(buffer, 0, length);
					}
					fos.close();
				}
			}
			zis.close();
			fis.close();
		} catch (IOException e) {
			throw e;
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static long getFileSize(String fileDirectory, String fileName) {
		String filePath = fileDirectory + fileName;
		File file = new File(filePath);
		if (!file.exists() || !file.isFile()) return 0L;
		return file.length();
	}
}