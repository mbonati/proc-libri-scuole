package com.sbd.procscuola;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sbd.procscuola.db.MapperScuole;
import com.sbd.procscuola.storage.DataStorageClient;
import com.sbd.procscuola.storage.impl.dropbox.DropBoxDataStorage;

import ch.qos.logback.classic.Level;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {

		LOG.info("Starting procedure...");

		try {

			// Load the configuration
			String configFileName = "./config.json"; // default
			if (args.length > 0) {
				configFileName = args[0];
			}
			LOG.info("Loading configuration...");
			JSONObject configuration = Utils.loadConfig(configFileName);

			setupLogger(configuration);
			processDatabase(configuration);
			processInputFiles(configuration);
			uploadOutputFiles(configuration);

		} catch (Throwable th) {
			LOG.error("Procedure fatal error:{}", th.getMessage(), th);
		}

	}

	private static void setupLogger(JSONObject configuration) {
		if (configuration.containsKey("log")) {
			JSONObject logConfig = configuration.getJSONObject("log");
			if (logConfig.containsKey("level")) {
				String logLevel = logConfig.getString("level");
				Level level = null;
				if (logLevel.equalsIgnoreCase("debug")) {
					level = Level.DEBUG;
				} else if (logLevel.equalsIgnoreCase("info")) {
					level = Level.INFO;
				} else if (logLevel.equalsIgnoreCase("trace")) {
					level = Level.TRACE;
				} else if (logLevel.equalsIgnoreCase("off")) {
					level = Level.OFF;
				} else if (logLevel.equalsIgnoreCase("warn")) {
					level = Level.WARN;
				}
				ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
						.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
				root.setLevel(level);
			}

		}
	}

	/**
	 * Create and import database files for SCUOLA
	 * 
	 * @param configuration
	 * @throws Exception
	 */
	private static void processDatabase(JSONObject configuration) throws Exception {

		JSONObject dbConfig = configuration.getJSONObject("database");
		boolean initialize = dbConfig.getBoolean("initialize");
		boolean process = dbConfig.getBoolean("process");

		if (!process) {
			LOG.warn("Database process is disabled. Skipping this task...");
			return;
		}

		JSONArray files = dbConfig.getJSONArray("files");

		MapperScuole ms = new MapperScuole();

		if (initialize && files.size()>0) {
			LOG.info("Initializing database...");

			// kill old database
			ms.killDatabase();

			// create the database
			ms.createDatabase();

			// clean the database
			ms.cleanDatabase();

			LOG.info("Database initialized successfully.");
		} else {
			LOG.warn("Database initialization is disabled.");
		}

		if ((files != null) && (files.size() > 0)) {
			for (int i = 0; i < files.size(); i++) {
				String fileName = files.getString(i);
				LOG.info("Importing file {}...", fileName);
				ms.loadFile(fileName);
				LOG.info("File {} import done.", fileName);
			}
		}

		LOG.info("Database is now ready to use.");

	}

	private static void processInputFiles(JSONObject configuration) throws Exception {

		LOG.info("Processing input files...");

		JSONObject outConfig = configuration.getJSONObject("output");
		boolean useSubfolder = outConfig.getBoolean("subfolder");
		String outFolder = outConfig.getString("folder");

		ListaTestiProcessor ltp = new ListaTestiProcessor(configuration);

		// Process input files
		JSONObject inputConfig = configuration.getJSONObject("input");
		JSONArray files = inputConfig.getJSONArray("files");
		if ((files != null) && (files.size() > 0)) {
			for (int i = 0; i < files.size(); i++) {
				long startTime = System.currentTimeMillis();
				String fileName = files.getString(i);
				LOG.info("Importing file {}...", fileName);
				ltp.startProcess(new File(fileName), outFolder, useSubfolder);
				long endTime = System.currentTimeMillis();
				LOG.info("File {} import done. Total {}ms proc time.", fileName, (endTime - startTime));
			}
		}

		LOG.info("Process completed successfully.");

	}

	private static void uploadOutputFiles(JSONObject configuration) throws Exception {
		JSONObject uploaderConfig = configuration.getJSONObject("uploader");
		if (uploaderConfig==null){
			LOG.info("No uploader defined into configuration. Skipping this task.");
			return;
		}
		
		String uploaderType = null;
		if (uploaderConfig.containsKey("type")){
			uploaderType = uploaderConfig.getString("type");
		}

		if (uploaderType!=null){
			LOG.info("Uploader {} found in configuration.", uploaderType);
			uploadOutputFiles(configuration, uploaderType);
		} else {
			LOG.info("Uploader type not defined in configuration. Skipping this task");
		}
		
	}
	
	private static void uploadOutputFiles(JSONObject configuration, String uploaderType) throws Exception {
		LOG.debug("uploadOutputFiles called for {}.", uploaderType);

		DataStorageClient dsc = createStorageForType(uploaderType);
		LOG.debug("DataStorageClient for {} is {}", uploaderType, dsc);
		dsc.setup(configuration);

		JSONObject outConfig = configuration.getJSONObject("output");
		boolean useSubfolder = outConfig.getBoolean("subfolder");
		String outFolder = outConfig.getString("folder");

		LOG.info("Starting uploading data...");

		dsc.uploadFolder(new File(outFolder));

		LOG.debug("uploadOutputFiles done {}.", uploaderType);
	}

	private static DataStorageClient createStorageForType(String uploaderType) {
		if (uploaderType.equalsIgnoreCase("dropbox")){
			return new DropBoxDataStorage();
		} else {
			return null;
		}
	}
	
	
}
