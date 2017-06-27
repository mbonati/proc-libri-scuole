package com.sbd.procscuola.test;

import org.apache.commons.io.FilenameUtils;

public class TestCommonsIO {

	public static void main(String[] args) {
		String originalPath = "\\AG\\AG_201706178.txt";
		out(originalPath);
		out(FilenameUtils.getBaseName(originalPath));
		out(FilenameUtils.getPath(originalPath));
		out(toUnixPath(originalPath));
	}
	
	private static String toUnixPath(String filePath){
		String baseName = FilenameUtils.getBaseName(filePath);
		String path = FilenameUtils.getPath(filePath);
		String ext = FilenameUtils.getExtension(filePath);
		return FilenameUtils.normalize(path) +  baseName +"." + ext;
	}
	
	private static void out(String msg){
		System.out.println(msg);
	}

}
