package com.dee.zpzrs.dal.mysql;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataLoader {

	private BufferedReader _SourceData;
	private String _fileName;
	private StringBuilder _sqlStatement;
	private MySQLHelper _sqlHelper;
	
	public DataLoader(String dataFileDir, MySQLHelper mysqlHelper){
		_sqlHelper = mysqlHelper;
		try {
			_SourceData = new BufferedReader(new FileReader(dataFileDir));
			_fileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1 ,dataFileDir.lastIndexOf('.'));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Load data from csv file to MySQL database.
	 * @param tableType Value with "TEMPORARY" indicate that the table is created as temporary table or just value with empty string.
	 * @throws IOException
	 * @throws SQLException
	 */
	public void CSVToMySQL(String tableType) throws IOException, SQLException{
		String record = null;
		String[] fields = null;
		StringBuilder fieldsUnit = new StringBuilder("(");
		
		fields = _SourceData.readLine().split(",");//create table
		_sqlStatement = new StringBuilder("CREATE " + tableType + " TABLE " + _fileName + " (" + _fileName + "_id INT UNSIGNED AUTO_INCREMENT,");
		for(int i = 0; i < fields.length; i++){
			if(i != (fields.length - 1)){
				fieldsUnit.append(fields[i] + ",");
			}else{
				fieldsUnit.append(fields[i]);
			}
			_sqlStatement.append(fields[i] + " text,");
		}
		fieldsUnit.append(")");
		_sqlStatement.append("PRIMARY KEY (" + _fileName + "_id))");
		System.out.println("[Tracing] Create table statement built as: " + _sqlStatement.toString());
		
		_sqlHelper.Execute(_sqlStatement.toString());
		
		int jobCounter = 0;
        while ((record = _SourceData.readLine()) != null){//insert data
        	System.out.println("\n\n======= Job " + (jobCounter+1) + " processing =======");
            String[] Elements = record.split(",");
            
            _sqlStatement = new StringBuilder("INSERT INTO " + _fileName + fieldsUnit.toString() + "VALUES (\"");
            for(int i = 0; i < Elements.length; i++){
            	if(i != (Elements.length - 1)){
            		_sqlStatement.append(Elements[i] + "\",\"");
    			}else{
    				_sqlStatement.append(Elements[i] + "\")");
    			}
            }
            _sqlHelper.Execute(_sqlStatement.toString());
            
            jobCounter++;
            System.out.println("[Tracing]Job " + jobCounter + " is done!");
        }
	}
	
	/**
	 * Get a record from a table.
	 * @param tableName The name of the table contains query record.
	 * @param recordKey The identity id (Primary key) of the record.
	 * @return A ResultSet handling the record selected from MySQL database.
	 * @throws SQLException
	 */
	public ResultSet GetARecord(String tableName, int recordKey) throws SQLException{
		_sqlStatement = new StringBuilder("SELECT * FROM " + tableName + " where " + tableName + "_id=" + recordKey);
		return _sqlHelper.ExecuteQuery(_sqlStatement.toString());
	}
	
	/**
	 * Get the field of a table.
	 * @param tableName The name of the table.
	 * @return A ResultSet handling the field of the table.
	 * @throws SQLException
	 */
	public ResultSet GetField(String tableName) throws SQLException{
		_sqlStatement = new StringBuilder("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = '" + tableName + "'");
		ResultSet dataHandler = _sqlHelper.ExecuteQuery(_sqlStatement.toString());
		return dataHandler;
	}
	
	/**
	 * Get the record count number of the table.
	 * @param tableName The name of the table.
	 * @return An integer value of the count result.
	 * @throws NumberFormatException
	 * @throws SQLException
	 */
	public int CountRecords(String tableName) throws NumberFormatException, SQLException{
		_sqlStatement = new StringBuilder("select count(*) from " + tableName);
		ResultSet dataHandler = _sqlHelper.ExecuteQuery(_sqlStatement.toString());
		dataHandler.next();
		return Integer.parseInt(dataHandler.getString(1));
	}
	
	
	
	/**
	 * Close the buffer steam used for loading files.
	 * @throws IOException
	 */
	public void Close() throws IOException{
		_SourceData.close();
	}
}
