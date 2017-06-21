package com.sbd.procscuola.db;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperScuole {

	private static final Logger LOG = LoggerFactory.getLogger(MapperScuole.class);
	
	private ScuolaDef lastScuolaDefLookup;
	
	public MapperScuole() {

	}
	
	/**
	 * Cancella tutti i records presenti sul database
	 */
	public void cleanDatabase(){
		LOG.debug("Cleaning database...");
		Connection connection = null;
		Statement stm = null;
		try {
			connection = DatabaseManager.getInstance().getConnection();
			stm  = connection.createStatement();
			stm.execute("DELETE FROM scuole");
			LOG.debug("Cleaning database done.");
		} catch (Exception ex){
			LOG.debug("Cleaning database error: {}", ex.getMessage(), ex);
		} finally {
			DatabaseManager.close(stm, connection);
		}
	}
	
	/**
	 * Carica record scuole da file CSV
	 * 
	 * @param sourceFileName
	 * @throws IOException
	 * @throws SQLException
	 */
	public void loadFile(String sourceFileName) throws IOException, SQLException {
		LOG.debug("Loading file {}", sourceFileName);
		
		long counter = 0;

		Reader in = new FileReader(sourceFileName);
		Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().withTrim().parse(in);

		Connection connection = DatabaseManager.getInstance().getConnection();
		PreparedStatement preparedStatement = null;
		try {
			connection.setAutoCommit(false);
			String sqlInsert = "INSERT INTO scuole (CODICESCUOLA,ANNOSCOLASTICO,AREAGEOGRAFICA,REGIONE,PROVINCIA,CODICEISTITUTORIFERIMENTO,DENOMINAZIONEISTITUTORIFERIMENTO,DENOMINAZIONESCUOLA,INDIRIZZOSCUOLA,CAPSCUOLA,CODICECOMUNESCUOLA,DESCRIZIONECOMUNE,DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA,INDIRIZZOEMAILSCUOLA,SITOWEBSCUOLA,INDIRIZZOPECSCUOLA,DESCRIZIONECARATTERISTICASCUOLA,INDICAZIONESEDEDIRETTIVO,INDICAZIONESEDEOMNICOMPRENSIVO)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			preparedStatement = connection.prepareStatement(sqlInsert);
			for (CSVRecord record : records) {
				
				/**
				INSERT INTO (
				CODICESCUOLA, 
				ANNOSCOLASTICO, 
				AREAGEOGRAFICA,
				REGIONE,
				PROVINCIA,
				CODICEISTITUTORIFERIMENTO,
				DENOMINAZIONEISTITUTORIFERIMENTO,
				DENOMINAZIONESCUOLA,
				INDIRIZZOSCUOLA,
				CAPSCUOLA,
				CODICECOMUNESCUOLA,
				DESCRIZIONECOMUNE,
				DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA,
				INDIRIZZOEMAILSCUOLA,
				SITOWEBSCUOLA,
				INDIRIZZOPECSCUOLA,
				DESCRIZIONECARATTERISTICASCUOLA,
				INDICAZIONESEDEDIRETTIVO,
				INDICAZIONESEDEOMNICOMPRENSIVO
				) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
				**/
				
				preparedStatement.setString(1,  record.get("CODICESCUOLA"));
				preparedStatement.setString(2,  record.get("ANNOSCOLASTICO", ""));
				preparedStatement.setString(3,  record.get("AREAGEOGRAFICA", ""));
				preparedStatement.setString(4,  record.get("REGIONE", ""));
				preparedStatement.setString(5,  record.get("PROVINCIA", ""));
				preparedStatement.setString(6,  record.get("CODICEISTITUTORIFERIMENTO", ""));
				preparedStatement.setString(7,  record.get("DENOMINAZIONEISTITUTORIFERIMENTO", ""));
				preparedStatement.setString(8,  record.get("DENOMINAZIONESCUOLA", ""));
				preparedStatement.setString(9,  record.get("INDIRIZZOSCUOLA", ""));
				preparedStatement.setString(10, record.get("CAPSCUOLA", ""));
				preparedStatement.setString(11, record.get("CODICECOMUNESCUOLA", ""));
				preparedStatement.setString(12, record.get("DESCRIZIONECOMUNE", ""));
				preparedStatement.setString(13, record.get("DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA", ""));
				preparedStatement.setString(14, record.get("INDIRIZZOEMAILSCUOLA", ""));
				preparedStatement.setString(15, record.get("SITOWEBSCUOLA", ""));
				preparedStatement.setString(16, record.get("INDIRIZZOPECSCUOLA", ""));
				preparedStatement.setString(17, record.get("DESCRIZIONECARATTERISTICASCUOLA", ""));
				preparedStatement.setString(18, record.get("INDICAZIONESEDEDIRETTIVO", ""));
				preparedStatement.setString(19, record.get("INDICAZIONESEDEOMNICOMPRENSIVO", ""));
				preparedStatement.addBatch();
				
				counter++;
				
				if(counter % 5000 == 0){
					preparedStatement.executeBatch();
			    	LOG.debug("Imported {} records", counter);
				}
			}
			
			preparedStatement.executeBatch();
			connection.commit();
		} catch (Exception ex){
			LOG.error("Error importing data: {}", ex.getMessage(), ex);
		} finally {
			connection.setAutoCommit(true);
			DatabaseManager.close(preparedStatement, connection);
		}
		
		LOG.debug("File load done. {} items loaded.", counter);
	}
	
	/**
	 * Restituisce il numero di record scuole presenti nel database
	 * @return
	 * @throws SQLException
	 */
	public long count() throws SQLException {
		Connection connection = DatabaseManager.getInstance().getConnection();
		Statement preparedStatement = null;
		ResultSet rst = null;
		try {
			preparedStatement = connection.createStatement();
			rst = preparedStatement.executeQuery("SELECT COUNT(*) AS RECORDCOUNT FROM scuole");
			while(rst.next()){
				long ret = rst.getLong("RECORDCOUNT");
				return ret;
			}
			return 0;
		} catch (Exception ex){
			LOG.error("Error getting count: {}", ex.getMessage(), ex);
			throw ex;
		} finally {
			DatabaseManager.close(rst, preparedStatement, connection);
		}
	}
	
	/**
	 * Delete the current database (if exists)
	 * @throws IOException
	 * @throws SQLException
	 */
	public void killDatabase() throws IOException  {
		DatabaseManager.killDatabase();
	}
	
	/**
	 * Create a new fresh database
	 * @throws IOException
	 * @throws SQLException
	 * @throws URISyntaxException 
	 */
	public void createDatabase() throws IOException, SQLException, URISyntaxException {
		
		LOG.debug("Creating SCUOLE table...");
		String sql = getSQLFromFile("SCUOLE.createtable.sql");
		executeDSLScript(sql);
		LOG.debug("SCUOLE table created.");
		
	}
	
	/**
	 * Execute an SQL script
	 * @param sql
	 * @throws SQLException
	 */
	private void executeDSLScript(String sql) throws SQLException {
		LOG.debug("Executing script: \n{}",sql);
		Connection connection = DatabaseManager.getInstance().getConnection();
		Statement preparedStatement = null;
		try {
			preparedStatement = connection.createStatement();
			preparedStatement.execute(sql);
		} catch (Exception ex){
			LOG.error("Error executing sql script: {}", ex.getMessage(), ex);
			throw ex;
		} finally {
			DatabaseManager.close(preparedStatement, connection);
		}
	}
	
	/**
	 * Retrieve an SQL script from SQL resource folder
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	private String getSQLFromFile(String fileName) throws IOException, URISyntaxException {
		InputStream is = this.getClass().getResourceAsStream(fileName);
		String content = IOUtils.toString(is, "UTF-8"); 
		is.close();
		return content;
	}

	public ScuolaDef lookupScuolaFormCodice(String codiceScuola, Connection lookupConnection) throws SQLException{

		if ((lastScuolaDefLookup!=null) && lastScuolaDefLookup.getCodiceScuola().equals(codiceScuola)){
			return lastScuolaDefLookup;
		}

		Connection connection = lookupConnection;
		Statement preparedStatement = null;
		ResultSet rst = null;
		try {
			preparedStatement = connection.createStatement();
			rst = preparedStatement.executeQuery("SELECT * FROM scuole WHERE codicescuola = '" + codiceScuola + "'");
			while(rst.next()){
				ScuolaDef sd = buildData(rst.getString("CODICESCUOLA"), rst);
				//ScuolaDef sd = new ScuolaDef(rst.getString("CODICESCUOLA"), datiScuola);
				lastScuolaDefLookup = sd;
				return sd;
			}
			return null;
		} catch (Exception ex){
			LOG.error("Error getting scuolaDesc: {}", ex.getMessage(), ex);
			throw ex;
		} finally {
			DatabaseManager.close(rst, preparedStatement, null);
		}	
	}
	
	/**
	 * Build data map with record values
	 * @param rst
	 * @return
	 * @throws SQLException
	 */
	private ScuolaDef buildData(String codiceScuola, ResultSet rst) throws SQLException {
		
		List<String> names = new ArrayList<String>();
		List<String> values = new ArrayList<String>();
		Map<String, String> data = new HashMap<String, String>();
		ResultSetMetaData metadata = rst.getMetaData();
		int colCount = metadata.getColumnCount();
		for (int i=1;i<=colCount;i++){
			String colName = metadata.getColumnName(i);
			String value = rst.getString(i);
//			if (value.indexOf(",")>-1){
//				//quote string
//				value = "\"" + value + "\"";
//			}
			data.put(colName, value);
			names.add(colName);
			values.add(value);
		}
		ScuolaDef sd = new ScuolaDef(rst.getString("CODICESCUOLA"), data, names, values);
		return sd;
	}
	
	/**
	 * Find a scuolaData by CODICESCUOLA
	 * @param codiceScuola
	 * @return
	 * @throws SQLException
	 */
	public ScuolaDef lookupScuolaFormCodice(String codiceScuola) throws SQLException{
		Connection connection = DatabaseManager.getInstance().getConnection();
		try {
			return lookupScuolaFormCodice(codiceScuola, connection);
		} catch (Exception ex){
			LOG.error("Error getting scuolaDesc: {}", ex.getMessage(), ex);
			throw ex;
		} finally {
			DatabaseManager.close(null, null, connection);
		}	
	}

	
}
