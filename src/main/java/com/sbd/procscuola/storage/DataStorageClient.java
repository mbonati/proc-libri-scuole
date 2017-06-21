package com.sbd.procscuola.storage;

import java.io.File;

import net.sf.json.JSONObject;

public interface DataStorageClient {
	
	public void setup(JSONObject configuration) throws Exception;
	
	public void uploadFile(File file, File basePath) throws Exception;
	
	public void uploadFolder(File folder) throws Exception;

}
