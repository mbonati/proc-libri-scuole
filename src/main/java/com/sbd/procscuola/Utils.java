package com.sbd.procscuola;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;

import net.sf.json.JSONObject;

public class Utils {

	public static JSONObject loadConfig(String configFileName) throws IOException {
		File f = new File(configFileName);
		InputStream in = new FileInputStream(f);
		try {
			String jsonStr = (IOUtils.toString(in));
			return JSONObject.fromObject(jsonStr);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	public static void compressFile(File inputFile, File outputFile) {
		byte[] buffer = new byte[1024];
		ZipOutputStream zos = null;
		FileInputStream in = null;
		try {

			FileOutputStream fos = new FileOutputStream(outputFile);
			zos = new ZipOutputStream(fos);
			ZipEntry ze = new ZipEntry(inputFile.getName());
			zos.putNextEntry(ze);
			in = new FileInputStream(inputFile);

			int len;
			while ((len = in.read(buffer)) > 0) {
				zos.write(buffer, 0, len);
			}

			in.close();
			zos.closeEntry();

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				zos.close();
			} catch (Exception ex) {
			}
			try {
				in.close();
			} catch (Exception ex) {
			}
		}
	}
	
	

}
