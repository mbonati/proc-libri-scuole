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
import com.sbd.procscuola.storage.DataStorageClient;

import net.sf.json.JSONObject;

public class DropBoxDataStorage implements DataStorageClient {

	private static final Logger LOG = LoggerFactory.getLogger(DropBoxDataStorage.class);

	DbxClient dbxClient;
	JSONObject configuration;
	
	public DropBoxDataStorage() {
	}

	public void setup(JSONObject configuration) throws Exception {
		LOG.info("Initilazing {}...", this.getClass().getSimpleName());

		String accessToken = null;//"mXP6cC_TOsoAAAAAAAAAvNS20k9IyfGOMQVanjFD5wEwnqkzRj_eAvKB514GpLCM";

		this.configuration = configuration;
		JSONObject uploaderConfig = null;
		if (this.configuration != null) {
			uploaderConfig = configuration.getJSONObject("uploader");
			accessToken = uploaderConfig.getString("accessToken");
		} else {
			LOG.info("Configuration not found for {} uploader.", this.getClass().getSimpleName());
			return;
		}

		DbxRequestConfig config = new DbxRequestConfig("ProcLibriScuole.DropBoxDataStorage/1.0",
				Locale.getDefault().toString());

		this.dbxClient = new DbxClient(config, accessToken);

		LOG.info("{} intialized successfully.", this.getClass().getSimpleName());
	}

	@Override
	public void uploadFile(File file, File basePath) throws Exception {
		long startUploadTime = System.currentTimeMillis();
		LOG.info("uploading file {}...", file.getName());

        FileInputStream inputStream = new FileInputStream(file);
        try {
        	String baseFolderPath = basePath.getAbsolutePath();
        	String fileFolderPath = file.getAbsolutePath();
        	String fileBasePath = fileFolderPath.substring(baseFolderPath.length());
        	String fileNameOriginal = fileBasePath;
        	String fileName = toUnixPath("/" + fileBasePath);
        	LOG.info("Filename is {} (original={})", fileName, fileNameOriginal);
            DbxEntry.File uploadedFile = dbxClient.uploadFile(fileName,
                DbxWriteMode.add(), file.length(), inputStream);
        } finally {
            inputStream.close();
        }

		long endUploadTime = System.currentTimeMillis();
		LOG.info("file {} uploaded. Total {}s", file.getName(), (endUploadTime - startUploadTime) / 1000);
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
				uploadFile(file, folder);
			}
		}

		long endUploadTime = System.currentTimeMillis();
		LOG.info("Folder {} uploaded. Total {}s", folder.getName(), (endUploadTime - startUploadTime) / 1000);
	}

}
