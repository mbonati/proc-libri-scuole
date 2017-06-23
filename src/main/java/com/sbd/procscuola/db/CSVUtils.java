package com.sbd.procscuola.db;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sbd.procscuola.ListaTestiProcessor;

import net.sf.json.JSONObject;

public class CSVUtils {

	private static final Logger LOG = LoggerFactory.getLogger(CSVUtils.class);

	private static final char DEFAULT_SEPARATOR = ',';

	public static void writeLine(Writer w, List<String> values) throws IOException {
		writeLine(w, values, DEFAULT_SEPARATOR, ' ');
	}

	public static void writeLine(Writer w, List<String> values, char separators) throws IOException {
		writeLine(w, values, separators, ' ');
	}

	// https://tools.ietf.org/html/rfc4180
	private static String followCVSformat(String value) {

		String result = value;
		 if (result.contains("\"")) {
		 result = result.replace("\"", "\"\"");
		 }
//		if (result.contains(",")) {
//			result = "\"" + result + "\"";
//		}
		return result;

	}

	public static void writeLine(Writer w, List<String> values, char separators, char customQuote) throws IOException {

		boolean first = true;

		// default customQuote is empty

		if (separators == ' ') {
			separators = DEFAULT_SEPARATOR;
		}

		StringBuilder sb = new StringBuilder();
		for (String value : values) {
			if (!first) {
				sb.append(separators);
			}
			if (customQuote == ' ') {
				sb.append(followCVSformat(value));
			} else {
				sb.append(customQuote).append(followCVSformat(value)).append(customQuote);
			}

			first = false;
		}
		sb.append("\n");
		w.append(sb.toString());

	}
	
	private String quoteString(String valueStr) {
		return "\"" + valueStr +"\"" ;
	}


	public static CSVFormat getCSVFormatFor(String format) {
		if (format.equalsIgnoreCase("RFC4180")) {
			return CSVFormat.RFC4180; // RFC4180 See
										// https://tools.ietf.org/html/rfc4180
		} else if (format.equalsIgnoreCase("tdf")) {
			return CSVFormat.TDF; // tab delimited format
		} else if (format.equalsIgnoreCase("default")) {
			return CSVFormat.DEFAULT; // RFC4180 but allowing empty lines
		} else if (format.equalsIgnoreCase("excel")) {
			return CSVFormat.EXCEL; // Excel file format (using a comma as the
									// value delimiter).
		}
		return CSVFormat.RFC4180;
	}

	public static CSVFormat buildReaderFromConfig(JSONObject inputConfig) {
		// Iterable<CSVRecord> records =
		// CSVFormat.RFC4180.withDelimiter(';').withQuote('"').withFirstRecordAsHeader().withTrim().parse(in);
		// Iterable<CSVRecord> records =
		// CSVFormat.TDF.withFirstRecordAsHeader().withTrim().parse(in);

		// "format": "RFC4180",
		// "delimiter" : ";",
		// "quote" : "\"",
		// "trim" : true

		//return CSVFormat.RFC4180.withDelimiter(';').withQuote('"').withFirstRecordAsHeader().withTrim();

		LOG.info("CSV reader format is {}", inputConfig);

		CSVFormat reader = CSVFormat.RFC4180;
		if (inputConfig.containsKey("format")) {
			reader = CSVUtils.getCSVFormatFor(inputConfig.getString("format"));
		}

		if (inputConfig.containsKey("delimiter")) {
			reader = reader.withDelimiter(inputConfig.getString("delimiter").charAt(0));
		}

		if (inputConfig.containsKey("quote")) {
			reader = reader.withQuote(inputConfig.getString("quote").charAt(0));
		}

		if (inputConfig.containsKey("trim")) {
			reader = reader.withTrim(inputConfig.getBoolean("trim"));
		}
		
		return reader.withFirstRecordAsHeader();
	}

}
