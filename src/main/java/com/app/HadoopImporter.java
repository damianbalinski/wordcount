package com.app;

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
public class HadoopImporter {


	public static void main(String[] args) {
		String log4jConfPath = "src/main/resources/application.properties";
		PropertyConfigurator.configure(log4jConfPath);
		if (args.length != 5) {
			System.out.println("Invalid usage, try importer <downloadDirectory> <hadoopDirectory> <fileName> <fileUrl>");
		}
//		String zipDirectory = "C:/src/java/wordcount/zip/";
//		String fileDirectory = "C:/src/java/wordcount/dest/";
//		String hadoopDirectory = "/user/hduser/input3";
//		String fileName = "book_levels.csv";
//		String fileUrl = "https://storage.googleapis.com/kaggle-data-sets/3173499/5500424/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230424%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230424T080132Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=14797ca34d51445601c77bf65505b546e1afa3af38baa47fbfa7549cbe98a6d9a296326ce63fba5c3b58d5f9fe410be0932917856bb921c7509eea896a822b460d87a5780130b7951d5f0aaf4ce8b4ef73e0ccf428a6b0c0e53e6a181de7313e6f3495ea8aa0dbd69cda04adb01ffc09d4c7663c090e8d36eefaed6d2420b8c327498aeee7f0c1cbc2752d7af0aec7172abfa0d9670eb66cdadf9cab3a13b18de1959476540425b5d34eae955c4e011a574d6b178f86b5a4f5a4ea6aa5aeb398f9b46e8cfeb65bb33608308421b891386fc427c2da87948eb578432040abaa02afbb39fad7cbaa10b30f2a19ff55098443bdf3db2699a6c38a3219f07e91a199";
//		String zipFileName = "archive.zip";

		String downloadDirectory = args[0];
		String hadoopDirectory = args[1];
		String downloadFileName = args[2];
		String fileName = args[3];
		String fileUrl = args[4];

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
		System.out.println(cmd);
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