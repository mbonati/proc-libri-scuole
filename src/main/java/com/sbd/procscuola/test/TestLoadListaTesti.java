package com.sbd.procscuola.test;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sbd.procscuola.ListaTestiProcessor;
import com.sbd.procscuola.Utils;

import net.sf.json.JSONObject;

public class TestLoadListaTesti {
	
	private static final Logger LOG = LoggerFactory.getLogger(TestLoadListaTesti.class);


	public static void main(String[] args) throws Exception {
		JSONObject test = Utils.loadConfig("./config.json");
		System.out.println(test.get("database"));
		
		String path = "/Users/marcobonati/Develop/sources/Personal/sbd_lista_scuole/";
//		String sourceFileName = path + "/Nazionale_AIE_Finale.txt";
		String sourceFileName = path + "Adozioni_MIUR_20170612.txt.001";
		
		
		ListaTestiProcessor ltp = new ListaTestiProcessor(null);
		ltp.startProcess(new File(sourceFileName), "./out/", true);
		
//		MapperScuole ms = new MapperScuole();
//		Connection connection = DatabaseManager.getInstance().getConnection();
//		
//		long start = System.currentTimeMillis();
//		System.out.println("Reading file...");
//		
//		String path = "/Users/marcobonati/Develop/sources/Personal/sbd_lista_scuole/";
//		String sourceFileName = path + "/Nazionale_AIE_Finale.txt";
//		
//		long counter = 0;
//		Reader in = new FileReader(sourceFileName);
//		Iterable<CSVRecord> records = CSVFormat.TDF.withFirstRecordAsHeader().withTrim().parse(in);
//		for (CSVRecord record : records) {
//			//Map<String, String> recordMap = record.toMap();
//			String codiceScuola = record.get("COD_SCU");
//			
//			ScuolaDef scuolaDesc = ms.lookupScuolaFormCodice(codiceScuola, connection);
//			if (scuolaDesc==null){
//				//System.out.println("ERRROR! " + codiceScuola);
//			} else {
//				//System.out.println(scuolaDesc);
//			}
//			counter++;
//			
//			if(counter % 100000 == 0){
//		    	LOG.info("Processed {} records", counter);
//		    	LOG.debug("Codice {} Provincia {}", scuolaDesc.getCodiceScuola(), scuolaDesc.getProvincia() );
//			}
//
//		}
//
//		long end = System.currentTimeMillis();
//		LOG.info("Finished in {}ms", (end-start));
		
	}

}
