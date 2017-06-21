package com.sbd.procscuola;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import net.sf.json.JSONObject;

public class Utils {
	
	public static JSONObject loadConfig(String configFileName) throws IOException{
		File f = new File(configFileName);
		InputStream in = new FileInputStream(f);
		 try {
		   String jsonStr = ( IOUtils.toString( in ) );
		   return JSONObject.fromObject(jsonStr);
		 } finally {
		   IOUtils.closeQuietly(in);
		 }
	}
	
}
