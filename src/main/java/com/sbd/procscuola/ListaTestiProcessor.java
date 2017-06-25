package com.sbd.procscuola;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sbd.procscuola.db.CSVUtils;
import com.sbd.procscuola.db.DatabaseManager;
import com.sbd.procscuola.db.MapperScuole;
import com.sbd.procscuola.db.ScuolaDef;

import net.sf.json.JSONObject;

public class ListaTestiProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(ListaTestiProcessor.class);

	MapperScuole mapperScuole;
	Connection dbConnection;
	List<ListaTestiProcessorProgressListener> listeners = new ArrayList<ListaTestiProcessorProgressListener>();

	List<String> header;
	Map<String, FileInfo> outputFiles = null;
	String outPath = "./out/";
	boolean useSubfolder = true;
	FileInfo latestFileInfo = null;
	JSONObject configuration;
	
	char separatorChar = ',';
	char quotesChar = '"';
	String outFileExt = "csv";
	
	public ListaTestiProcessor(JSONObject configuration) {
		this.configuration = configuration;
		
		JSONObject outConfig = configuration.getJSONObject("output");
		if (outConfig!=null){
			if (outConfig.containsKey("quote")){
				String quotesCharStr = outConfig.getString("quote");
				if (quotesCharStr.length()>0){
					this.quotesChar = quotesCharStr.charAt(0);
				}
			}
			if (outConfig.containsKey("separator")){
				String separatorStr = outConfig.getString("separator");
				this.separatorChar = separatorStr.charAt(0);
			}
			if (outConfig.containsKey("fileExt")){
				this.outFileExt = outConfig.getString("fileExt");
			}
		}
		
		LOG.info("The output format is quotes={} separator={}", quotesChar, separatorChar);
	}

	public void addListener(ListaTestiProcessorProgressListener listener) {
		listeners.add(listener);
	}

	public void removeListener(ListaTestiProcessorProgressListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Process input file and produce separate files
	 * 
	 * @param inputFile
	 * @param outputFolder
	 * @throws Exception
	 */
	public void startProcess(File inputFile, String outputFolder, boolean useSubfolder) throws Exception {
		LOG.info("startProcess for input={} to out folder={}", inputFile.getAbsolutePath(), outputFolder);

		outputFiles = new HashMap<String, FileInfo>();

		this.outPath = outputFolder;
		this.useSubfolder = useSubfolder;

		notifyListenersStartProcess();

		long startProcTime = System.currentTimeMillis();
		long partTime = System.currentTimeMillis();

		// get a dedicated process database connection
		dbConnection = DatabaseManager.getInstance().getConnection();

		// get a mapper
		MapperScuole ms = new MapperScuole();

		// get the configured row key
		String rowKeyField = getRowKeyField();
		LOG.info("The rowKey field configured is: '{}'", rowKeyField);

		// Load the input file
		long counter = 0;
		Reader in = new FileReader(inputFile);
		CSVFormat csvReader = buildReaderFromConfig();
		Iterable<CSVRecord> records = csvReader.parse(in);
		for (CSVRecord record : records) {

			LOG.trace("Processing record {}", (counter + 1));

			// Map<String, String> recordMap = record.toMap();
			String codiceScuola = record.get(rowKeyField);

			ScuolaDef scuolaDesc = ms.lookupScuolaFormCodice(codiceScuola, dbConnection);
			if (scuolaDesc == null) {
				processRecordInError(codiceScuola, record);
			} else {
				processRecord(scuolaDesc, record);
			}
			counter++;

			if (counter % 100000 == 0) {
				long endPartTime = System.currentTimeMillis();
				LOG.info("Processed {} records (time {}ms)", counter, (endPartTime - partTime));
				notifyListenersProcessProgress(counter);
				partTime = System.currentTimeMillis();
			}

			LOG.trace("Processed record {}", counter);
		}

		notifyListenersProcessComplete();

		long endProcTime = System.currentTimeMillis();

		LOG.info("Process finished. Time {}ms", (endProcTime - startProcTime));

	}

	private CSVFormat buildReaderFromConfig() {
		JSONObject inputConfig = getInputConfig();
		if (inputConfig != null) {
			return CSVUtils.buildReaderFromConfig(inputConfig);
		} else {
			return CSVFormat.RFC4180.withDelimiter(';').withQuote('"').withFirstRecordAsHeader().withTrim();
		}
	}

	private String getRowKeyField() {
		JSONObject inputConfig = getInputConfig();
		if (inputConfig != null && inputConfig.containsKey("rowKeyField")) {
			return inputConfig.getString("rowKeyField");
		}
		// else return default field
		return "COSCUO";
	}

	private JSONObject getInputConfig() {
		if (this.configuration != null) {
			return configuration.getJSONObject("input");
		} else {
			return null;
		}
	}

	/**
	 * Get a output file by ID
	 * 
	 * @param id
	 * @return
	 * @throws IOException
	 */
	private FileInfo getFileById(String id) throws IOException {
		if (outputFiles.containsKey(id)) {
			this.latestFileInfo = outputFiles.get(id);
			return this.latestFileInfo;
		} else {
			LOG.debug("Creating file for {}", id);
			SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMDD");
			String fileName = id + "_" + sdf.format(new Date()) + "." + getOutFileExtention();
			String path = null;
			if (this.useSubfolder) {
				path = outPath + "/" + id;
			} else {
				path = outPath;
			}
			ensureFolderExists(path);
			String completeFileName = path + "/" + fileName;
			File newFile = new File(completeFileName);
			LOG.debug("File for {} is: {}", id, newFile.getAbsolutePath());
			FileInfo fInfo = new FileInfo();
			fInfo.setup(id, newFile);
			outputFiles.put(id, fInfo);
			writeHeader(fInfo);
			this.latestFileInfo = fInfo;
			return this.latestFileInfo;
		}
	}

	private String getOutFileExtention() {
		return this.outFileExt;
	}

	private void ensureFolderExists(String path) throws IOException {
		File f = new File(path);
		if (!f.exists()) {
			FileUtils.forceMkdir(f);
		}
	}

	private void createHeader(ScuolaDef scuolaDesc, CSVRecord record) {
		this.header = new ArrayList<String>();
		this.header.addAll(Arrays.asList(record.names()));
		this.header.addAll(scuolaDesc.names());
	}

	private void processRecord(ScuolaDef scuolaDesc, CSVRecord record) throws IOException {
		if (this.header == null) {
			this.createHeader(scuolaDesc, record);
		}
		String provincia = scuolaDesc.getCodiceScuola().substring(0, 2);
		FileInfo f = null;
		if (this.latestFileInfo != null && this.latestFileInfo.id.equals(provincia)) {
			f = this.latestFileInfo;
		} else {
			f = getFileById(provincia);
		}
		// java.nio.file.Files.write(Paths.get(f.toURI()),
		// (scuolaDesc.toString() +"\n").getBytes("utf-8"),
		// StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		// FileUtils.writeStringToFile(f, buildOutputRecord(scuolaDesc, record),
		// true);
		//CSVUtils.writeLine(f.writer, buildOutputRecord(scuolaDesc, record),';','"');
		this.writeLine(f.writer, buildOutputRecord(scuolaDesc, record));
	}
	
	private void writeHeader(FileInfo f) throws IOException {
		//CSVUtils.writeLine(f.writer, this.header,separator,quotes);
		this.writeLine(f.writer, this.header);
	}
	
	
	private void writeLine(FileWriter writer, List<String> values) throws IOException {
		CSVUtils.writeLine(writer, values,separatorChar,quotesChar);
	}

	private List<String> buildOutputRecord(ScuolaDef scuolaDesc, CSVRecord record) {
		List<String> ret = new ArrayList<String>();
		ret.addAll(record.toList());
		ret.addAll(scuolaDesc.values());
		return ret;
	}

	private void processRecordInError(String codiceScuola, CSVRecord record) {
		// TODO!!
	}

	private void notifyListenersProcessProgress(long counter) {
		for (ListaTestiProcessorProgressListener listener : listeners) {
			try {
				listener.onProcessProgress(counter);
			} catch (Exception ex) {
				LOG.error("NOtifyListener error: {}", ex.getMessage(), ex);
			}
		}
	}

	private void notifyListenersStartProcess() {
		for (ListaTestiProcessorProgressListener listener : listeners) {
			try {
				listener.onProcessStarted();
			} catch (Exception ex) {
				LOG.error("NOtifyListener error: {}", ex.getMessage(), ex);
			}
		}
	}

	private void notifyListenersProcessComplete() {
		for (ListaTestiProcessorProgressListener listener : listeners) {
			try {
				listener.onProcessComplete();
			} catch (Exception ex) {
				LOG.error("NOtifyListener error: {}", ex.getMessage(), ex);
			}
		}
	}

	public interface ListaTestiProcessorProgressListener {
		public void onProcessStarted();

		public void onProcessComplete();

		public void onProcessAborted(Exception ex);

		public void onProcessProgress(long processedLines);
	}

	private static class FileInfo {
		public String id;
		public File file;
		public FileWriter writer;

		public FileInfo() {
		}

		public void setup(String id, File file) throws IOException {
			this.id = id;
			this.file = file;
			writer = new FileWriter(this.file);
		}
	}

}
