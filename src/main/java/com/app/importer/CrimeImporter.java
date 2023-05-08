package com.app.importer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.text.MessageFormat.format;

@Slf4j
public class CrimeImporter {

	public static void main(String[] args) {
		String log4jConfPath = "src/main/resources/application.properties";
		PropertyConfigurator.configure(log4jConfPath);

		if (args.length != 1) {
			System.out.println("Invalid usage, try CrimeImporter.java <YYYY>");
		}
		String year = args[0];

		// absolute path, \\ zamiast / i brak / na końcu, bo inaczej selenium nie pobiera
		String downloadDirectory = format("{0}\\src\\main\\resources\\crime\\{1}", System.getProperty("user.dir"), year);
		String hadoopDirectory = "/user/hduser/crime/";
		String downloadFileName = format("offenses-known-to-le-{0}.zip", year);
		String fileName = format("Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_{0}.xlsx", year);
		String fileNameCsv = format("Offenses_Known_to_Law_Enforcement_by_State_by_City_{0}.csv", year);
		String fileUrl = "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads";

		try {
			Instant start = Instant.now();
			log.info(format("Begin process of data acquisition"));

			if (Integer.parseInt(year) < 2020) {
				log.info("Trying to converse , fileName = {}", fileName);
				Duration conversionTime = converseFileBeforeApi(downloadDirectory, format("iowa_{0}.xls", year), fileNameCsv, downloadDirectory);
				log.info("File conversed successfully, fileName = {}", fileNameCsv);
				log.info("Conversion time = {} ms", conversionTime.toMillis());
			} else {
				log.info("Trying to download file, fileUrl = {}", fileUrl);
				Duration downloadTime = downloadFile(year, fileUrl, downloadDirectory, downloadFileName);
				log.info("File downloaded successfully, fileName = {}", downloadFileName);
				log.info("Download time = {} ms", downloadTime.toMillis());

				log.info("Trying to decompress zip archive, fileName = {}", downloadFileName);
				Duration unzipTime = unzipFile(downloadDirectory, downloadFileName, fileName, downloadDirectory);
				Long fileSize = getFileSize(downloadDirectory, fileName);
				log.info("File decompressed successfully, fileName = {}, fileSize = {} B", fileName, fileSize);
				log.info("Decompress time = {} ms", unzipTime.toMillis());

				log.info("Trying to converse , fileName = {}", fileName);
				Duration conversionTime = converseFile(downloadDirectory, fileName, fileNameCsv);
				log.info("File conversed successfully, fileName = {}", fileNameCsv);
				log.info("Conversion time = {} ms", conversionTime.toMillis());
			}

			log.info("Trying to copy file into container");
			Duration copyTime = copyFileIntoContainer(downloadDirectory, fileNameCsv);
			log.info("File copied into container successfully");
			log.info("Copy file = {} ms", copyTime.toMillis());

			log.info("Trying to put file into hadoop storage, hadoopDirectory = {}", hadoopDirectory);
			Duration putTime = putFileIntoHadoop(fileNameCsv, hadoopDirectory);
			log.info("Put file into hadoop storage successfully");
			log.info("Put time = {} ms", putTime.toMillis());

			log.info("Trying to replicate file on hadoop storage");
			Duration replicationTime = replicateFile(fileNameCsv, hadoopDirectory);
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

	private static Duration replicateFile(String csvFileName, String hadoopDirectory) throws Exception {
		Instant start = Instant.now();
		String filePath = hadoopDirectory + csvFileName;

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
		String filePath = fileDirectory + "\\" + fileName;
		String dockerPath = format("/tmp/{0}", fileName);
		String cmdCopyFile = format("docker cp {0} master:{1}", filePath, dockerPath);

		Process copyFileProcess = Runtime.getRuntime().exec(cmdCopyFile);
		InputStream copyFileInput = copyFileProcess.getInputStream();
		int copyFileExitCode = copyFileProcess.waitFor();
		log.info("Container copy file exitCode = {}, output = {}", copyFileExitCode, new String(IOUtils.toByteArray(copyFileInput)));

		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration putFileIntoHadoop(String csvFileName, String hadoopDirectory) throws Exception {
		// hadoop fs -put ./book_levels.csv /user/hduser/input
		Instant start = Instant.now();
		String filePath = "/tmp/" + csvFileName;

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

	private static Duration downloadFile(String specifiedYear, String fileUrl, String zipDirectory, String zipFileName) throws InterruptedException {
		Instant start = Instant.now();
		ChromeOptions options = new ChromeOptions();
		options.addArguments("--headless=new");
		options.setExperimentalOption("prefs", Collections.singletonMap("download.default_directory", "zipDirectory"));
		WebDriver driver = new ChromeDriver(options);
		driver.get(fileUrl);
		WebElement ciusDownloads = driver.findElement(By.id("ciusDownloads"));
		List<WebElement> buttons = ciusDownloads.findElements(By.cssSelector("button"));
		buttons.get(0).click();
		List<WebElement> years = driver.findElements(By.cssSelector("nb-option"));
		boolean yearIsAvailable = false;
		for (WebElement year : years) {
			if (year.getText().equals(specifiedYear)) {
				year.click();
				yearIsAvailable = true;
				break;
			}
		}
		if (!yearIsAvailable) {
			throw new RuntimeException(format("There is no data for given year: {0}", specifiedYear));
		}
		buttons.get(1).click();
		List<WebElement> types = driver.findElements(By.cssSelector("nb-option"));
		for (WebElement type : types) {
			if (type.getText().equals("Offenses Known to Law Enforcement")) {
				type.click();
				break;
			}
		}
		List<WebElement> download = ciusDownloads.findElements(By.cssSelector("a"));
		download.get(3).click();

		File dir = new File(zipDirectory);
		while (dir.listFiles() == null || Objects.requireNonNull(dir.listFiles(file -> file.getName().equals(zipFileName))).length == 0) {
			Thread.sleep(1000);
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration unzipFile(String zipDirectory, String zipFileName, String fileName, String fileDirectory) throws IOException {
		Instant start = Instant.now();
		String zipFilePath = zipDirectory + "/" + zipFileName;
		try (ZipInputStream zipInputStream = new ZipInputStream(Files.newInputStream(Paths.get(zipFilePath)))) {
			ZipEntry entry;
			while ((entry = zipInputStream.getNextEntry()) != null) {
				if (entry.getName().equals(fileName)) {
					try (FileOutputStream fos = new FileOutputStream(new File(fileDirectory, fileName))) {
						final byte[] buffer = new byte[1024];
						int length;
						while ((length = zipInputStream.read(buffer)) > 0) {
							fos.write(buffer, 0, length);
						}
						break;
					}
				}
			}
		} catch (IOException e) {
			System.out.println("Error during file extraction: " + e.getMessage());
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static long getFileSize(String fileDirectory, String fileName) {
		String filePath = fileDirectory + "/" + fileName;
		File file = new File(filePath);
		if (!file.exists() || !file.isFile()) return 0L;
		return file.length();
	}

	private static Duration converseFile(String downloadDirectory, String fileName, String fileNameCsv) {
		Instant start = Instant.now();
		String filePath = downloadDirectory + "/" + fileName;
		String filePathCsv = downloadDirectory + "/" + fileNameCsv;
		try (FileInputStream fileInputStream = new FileInputStream(filePath);
		     FileWriter fileWriter = new FileWriter(filePathCsv)) {
			fileWriter.write("City, Population, Violent Crime Total, Murder and nonnegligent manslaughter, Rape, Robbery, Aggravated assault, Property Crime Total, Burglary, Larceny-theft, Motor vehicle theft, Arson\n");
			Workbook workbook = WorkbookFactory.create(fileInputStream);
			Sheet sheet = workbook.getSheetAt(0);
			String state = "";
			for (Row row : sheet) {
				Iterator<Cell> cellIterator = row.cellIterator();
				if (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					if (!cell.getStringCellValue().equals("")) {
						state = cell.getStringCellValue();
					}
				}
				if (state.equalsIgnoreCase("Iowa")) {
					StringBuilder sb = new StringBuilder();
					while (cellIterator.hasNext()) {
						Cell cell = cellIterator.next();
						if (sb.length() > 0) {
							sb.append(",");
						}
						sb.append(cell);
					}
					sb.append("\n");
					fileWriter.write(sb.toString());
				}
			}
		} catch (IOException e) {
			System.out.println("Error during file conversion: " + e.getMessage());
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

	private static Duration converseFileBeforeApi(String downloadDirectory, String fileName, String fileNameCsv, String outputDirectory) {
		Instant start = Instant.now();
		String filePath = downloadDirectory + "/" + fileName;
		String filePathCsv = downloadDirectory + "/" + fileNameCsv;
		try (FileInputStream fileInputStream = new FileInputStream(filePath);
		     FileWriter fileWriter = new FileWriter(filePathCsv)) {
			fileWriter.write("City, Population, Violent Crime Total, Murder and nonnegligent manslaughter, Rape, Robbery, Aggravated assault, Property Crime Total, Burglary, Larceny-theft, Motor vehicle theft, Arson\n");
			Workbook workbook = WorkbookFactory.create(fileInputStream);
			Sheet sheet = workbook.getSheetAt(0);
			for (Row row : sheet) {
				if (row.getCell(0).getStringCellValue().equals("") || row.getCell(0).getStringCellValue().equals("City")) {
					continue;
				}
				StringBuilder sb = new StringBuilder();
				for (Cell cell : row) {
					if (sb.length() > 0) {
						sb.append(",");
					}
					sb.append(cell);
				}
				sb.append("\n");
				fileWriter.write(sb.toString());
			}
		} catch (IOException e) {
			System.out.println("Error during file conversion: " + e.getMessage());
		}
		Instant end = Instant.now();
		return Duration.between(start, end);
	}

}
