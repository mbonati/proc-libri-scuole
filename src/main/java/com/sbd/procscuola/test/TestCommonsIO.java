package com.sbd.procscuola.test;

import org.apache.commons.io.FilenameUtils;

import com.sbd.procscuola.storage.impl.dropbox.DropBoxDataStorage;

public class TestCommonsIO {

	public static void main(String[] args) {
		String originalPath = "\\XX\\AG\\AG_201706178.txt";
		out(originalPath);
		out(FilenameUtils.getBaseName(originalPath));
		out(FilenameUtils.getPath(originalPath));
		out(DropBoxDataStorage.toUnixPath(originalPath));
	}
	
//	private static String toUnixPath(String filePath){
//		String baseName = FilenameUtils.getBaseName(filePath);
//		String path = FilenameUtils.getPath(filePath);
//		String ext = FilenameUtils.getExtension(filePath);
//		return FilenameUtils.separatorsToUnix(FilenameUtils.normalize(path) +  baseName +"." + ext);
//	}
	
	private static void out(String msg){
		System.out.println(msg);
	}

}
