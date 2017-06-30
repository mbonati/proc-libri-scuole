package com.sbd.procscuola.storage.impl.dropbox;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dropbox.core.DbxClient;
import com.dropbox.core.DbxEntry;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.DbxWriteMode;
import com.sbd.procscuola.Utils;
import com.sbd.procscuola.storage.DataStorageClient;

import net.sf.json.JSONObject;

public class DropBoxDataStorage implements DataStorageClient {

	private static final Logger LOG = LoggerFactory.getLogger(DropBoxDataStorage.class);
	
	private static final int DEFAULT_MAX_RETRIES_COUNT = 3;
	
	private int retries = DEFAULT_MAX_RETRIES_COUNT;
	private boolean compress = false;
	
//	DbxClient dbxClient;
	JSONObject configuration;
	String accessToken = null;//"mXP6cC_TOsoAAAAAAAAAvNS20k9IyfGOMQVanjFD5wEwnqkzRj_eAvKB514GpLCM";
	
	public DropBoxDataStorage() {
	}

	public void setup(JSONObject configuration) throws Exception {
		LOG.info("Initilazing {}...", this.getClass().getSimpleName());

		this.configuration = configuration;
		JSONObject uploaderConfig = null;
		if (this.configuration != null) {
			uploaderConfig = configuration.getJSONObject("uploader");
			accessToken = uploaderConfig.getString("accessToken");
			if (uploaderConfig.containsKey("retries")){
				this.retries = uploaderConfig.getInt("retries");
			}
			if (uploaderConfig.containsKey("compress")){
				this.compress = uploaderConfig.getBoolean("compress");
			}
		} else {
			LOG.info("Configuration not found for {} uploader.", this.getClass().getSimpleName());
			return;
		}

//		DbxRequestConfig config = new DbxRequestConfig("ProcLibriScuole.DropBoxDataStorage/1.0",
//				Locale.getDefault().toString());
//		this.dbxClient = new DbxClient(config, accessToken);

		LOG.info("{} intialized successfully.", this.getClass().getSimpleName());
	}

	@Override
	public boolean uploadFile(File file, File basePath) throws Exception {
		long startUploadTime = System.currentTimeMillis();
		LOG.info("uploading file {}...", file.getName());
		
		DbxClient client = null;
		
        FileInputStream inputStream = new FileInputStream(file);
        try {
        	
        	if (this.compress){
	        	//first compress the file
	        	String sourcePath = FilenameUtils.getFullPath(file.getAbsolutePath());
	        	String zippedFileName = FilenameUtils.getBaseName(file.getName()) + ".zip";
	        	zippedFileName = sourcePath + "/" + zippedFileName;
	        	File zippedFile = new File(zippedFileName);
	        	Utils.compressFile(file, zippedFile);
	        	// then upload the zipped file, swap the variables
	        	file = zippedFile;
        	}
        	
        	client = getClient();
        	String baseFolderPath = basePath.getAbsolutePath();
        	String fileFolderPath = file.getAbsolutePath();
        	String fileBasePath = fileFolderPath.substring(baseFolderPath.length());
        	String fileNameOriginal = fileBasePath;
        	String fileName = "/" + toUnixPath(fileBasePath);
        	LOG.info("Filename is {} (original={})", fileName, fileNameOriginal);
            DbxEntry.File uploadedFile = client.uploadFile(fileName,
                DbxWriteMode.add(), file.length(), inputStream);
        } catch (Exception ex){
        	LOG.error("Error uploading file "+file.getName()+": {}", ex.getMessage(), ex);
        	return false;
        } finally {
            inputStream.close();
            client = null;
        }

		long endUploadTime = System.currentTimeMillis();
		LOG.info("file {} uploaded. Total {}s", file.getName(), (endUploadTime - startUploadTime) / 1000);
    	return true;
	}
	
	private DbxClient getClient(){
		DbxRequestConfig config = new DbxRequestConfig("ProcLibriScuole.DropBoxDataStorage/1.0",
				Locale.getDefault().toString());
		return new DbxClient(config, accessToken);
	}
	

	public static String toUnixPath(String filePath){
		String baseName = FilenameUtils.getBaseName(filePath);
		String path = FilenameUtils.getPath(filePath);
		String ext = FilenameUtils.getExtension(filePath);
		return FilenameUtils.separatorsToUnix(FilenameUtils.normalize(path) +  baseName +"." + ext);
	}

	
	@Override
	public void uploadFolder(File folder) throws Exception {
		LOG.info("Starting upload folder {}...", folder.getName());
		long startUploadTime = System.currentTimeMillis();

		final String[] SUFFIX = { "**" }; // use the suffix to filter
		Collection<File> files = FileUtils.listFiles(folder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
		for (File file:files){
			if (file.getName().equals(".DS_Store")){
				//skip
			} else {
				int attempts = 0;
				boolean uploadOk = false;
				while(!uploadOk || (attempts < retries)){
					uploadOk = uploadFile(file, folder);
					attempts++;
				}
			}
		}

		long endUploadTime = System.currentTimeMillis();
		LOG.info("Folder {} uploaded. Total {}s", folder.getName(), (endUploadTime - startUploadTime) / 1000);
	}

}
