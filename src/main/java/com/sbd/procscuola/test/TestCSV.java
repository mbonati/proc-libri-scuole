package com.sbd.procscuola.test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCSV {

	private static final Logger LOG = LoggerFactory.getLogger(TestCSV.class);

	public static void main(String[] args) throws Exception {

		String path = "/Users/marcobonati/Develop/sources/Personal/sbd_lista_scuole/";
		String sourceFileName = "Adozioni_MIUR_20170612.txt.001";
		
		File inputFile = new File(path + sourceFileName);
		
		long row = 1;
		Reader in = new FileReader(inputFile);
//		Iterable<CSVRecord> records = CSVFormat.TDF.withFirstRecordAsHeader().withTrim().parse(in);
		Iterable<CSVRecord> records = CSVFormat.RFC4180.withDelimiter(';').withQuote('"').withFirstRecordAsHeader().withTrim().parse(in);
		for (CSVRecord record : records) {
			LOG.debug("{} {}", row, record.get("COSCUO"));
			row++;
		}
		
	}

}
