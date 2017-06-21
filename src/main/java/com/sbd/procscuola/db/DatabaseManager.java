package com.sbd.procscuola.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseManager {
	
	private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);
	
	private static DatabaseManager _instance;
	private Connection dbConnection;
	
	public static DatabaseManager getInstance() throws SQLException {
		if (_instance==null){
			_instance = new DatabaseManager();
			_instance.initDB();
		}
		return _instance;
	}
	
	private DatabaseManager(){
	}
	
	private void initDB() throws SQLException {
		LOG.info("Initializing database...");
		try {
			dbConnection = DriverManager.getConnection("jdbc:hsqldb:file:./db/data;shutdown=true", "SA", "");
			dbConnection.close();
		} catch(SQLException  ex){
			LOG.error("Error during the database initialization: {}", ex.getMessage(), ex);
			throw ex;
		}
	}
	
	public Connection getConnection() throws SQLException{
		LOG.debug("Acquiring connection...");
		try {
			dbConnection = DriverManager.getConnection("jdbc:hsqldb:file:./db/data;shutdown=true", "SA", "");
			return dbConnection;
		} catch(SQLException  ex){
			LOG.error("Error during the database connection acquisition: {}", ex.getMessage(), ex);
			throw ex;
		}
	}
	
	public static void main(String[] args){
		org.hsqldb.util.DatabaseManager.main(args); 
		
	}

	public static void close(Statement stm, Connection connection) {
		try {
			stm.close();
		} catch (Exception ex){
		}
		try {
			connection.close();
		} catch (Exception ex){
		}
	}

	public static void close(ResultSet rst, Statement statement, Connection connection) {
		try {
			rst.close();
		} catch (Exception ex){
		}
		close(statement, connection);
	}

	public static void killDatabase() throws IOException {
		LOG.info("Killing database...");
		FileUtils.deleteDirectory(new File("./db"));
	}
	
}
