package com.sbd.procscuola.test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sbd.procscuola.db.DatabaseManager;
import com.sbd.procscuola.db.MapperScuole;
import com.sbd.procscuola.db.ScuolaDef;

public class ProcImportScuola {

	private static final Logger LOG = LoggerFactory.getLogger(ProcImportScuola.class);

	public static void main(String[] args) throws SQLException, IOException, URISyntaxException {
		LOG.info("Starting procedure...");
		
		long start = System.currentTimeMillis();
		
		String f1 = "SCUANAGRAFEPAR20170221.csv";
		String f2 = "SCUANAGRAFESTAT20170221.csv";
		String path = "/Users/marcobonati/Develop/sources/Personal/sbd_lista_scuole/";
		
		MapperScuole ms = new MapperScuole();
		
		// kill old database
		ms.killDatabase();
		
		// create the database
		ms.createDatabase();
		
		// clean the database
		ms.cleanDatabase();
		
		// load the CSV files
		ms.loadFile(path + f1);
		ms.loadFile(path + f2);
		
		// count total records imported
		long test = ms.count();
		LOG.info("Total records: {}", test);
		
		Connection connection = DatabaseManager.getInstance().getConnection();

		ScuolaDef scuolaDesc = ms.lookupScuolaFormCodice("AT1E00200G", connection);
		System.out.println(scuolaDesc);
		
		LOG.info("{}", scuolaDesc.values());
		LOG.info("{}", scuolaDesc.names());
		
//		scuolaDesc = ms.lookupScuolaFormCodice("CN1A03700X", connection);
//		System.out.println(scuolaDesc);
//
//		scuolaDesc = ms.lookupScuolaFormCodice("AG1E00500B", connection);
//		System.out.println(scuolaDesc);

		long end = System.currentTimeMillis();
		
		LOG.info("Process time: {}ms", (end-start));

		
	}


	
}
