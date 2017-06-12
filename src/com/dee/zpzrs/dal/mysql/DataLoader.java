package com.dee.zpzrs.dal.mysql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.dee.zpzrs.dal.ma.DataConstructor;
import com.dee.zpzrs.dal.ma.DataSession;

public class DataLoader {

	private BufferedReader _SourceData;
	private String _fileName;
	private StringBuilder _sqlStatement, _fieldsUnit;
	private MySQLHelper _sqlHelper;
	
	public DataLoader(String dataFileDir, MySQLHelper mysqlHelper){
		_sqlHelper = mysqlHelper;
		try {
			_SourceData = new BufferedReader(new FileReader(dataFileDir));
			_fileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1 ,dataFileDir.lastIndexOf('.'));
			
		} catch (IOException e) {
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
		
		fields = _SourceData.readLine().split(",");//create table
		_fieldsUnit = BuildFieldUnit(fields);
		
		CreateTable(tableType, fields, _fileName);
		
		int jobCounter = 0;
        while ((record = _SourceData.readLine()) != null){//insert data
        	System.out.println("\n\n======= Job " + (jobCounter+1) + " processing =======");
            String[] Elements = record.split(",");
            
            InsertARow(Elements, _fileName);
            
            jobCounter++;
            System.out.println("[Tracing]Job " + jobCounter + " is done!");
        }
	}
	
	/**
	 * Load data from an ESV type data file to MySQL database, each row of the data set will be create as a table in MySQL database.
	 * @param tableType Value with "TEMPORARY" indicate that the table is created as temporary table or just value with empty string.
	 * @param mainField The main field to identify the key of each row record.
	 * @param relations Give a map represent the relation ship between each field.
	 * @throws IOException
	 * @throws SQLException
	 */
	public void ESVToMySQL(String tableType, String mainField, Map<String, String> relations) throws IOException, SQLException{
		String record = null;
		String[] fields = null;
		DataConstructor dcServe;
		DataSession dsServe;
		
		fields = _SourceData.readLine().split(",");//create table
		
		_SourceData.readLine();
		int jobCounter = 0;
        while ((record = _SourceData.readLine()) != null){//insert data
        	System.out.println("\n\n======= Job " + (jobCounter+1) + " processing =======");
            
        	dcServe = new DataConstructor(record, fields, mainField);
        	dsServe = dcServe.ConsByRelation(relations);
        	_fieldsUnit = BuildFieldUnit(dsServe.GetFields());
        	DRSToMySQL(dsServe, "");
        	
            jobCounter++;
            System.out.println("[Tracing]Job " + jobCounter + " is done!");
        }
	}
	
	private void DRSToMySQL(DataSession dataRecordSession, String tableType) throws SQLException{
		CreateTable(tableType, dataRecordSession.GetFields(), dataRecordSession.sessionName);
		for(int i=0; i<dataRecordSession.dataRecordSize; i++){
			InsertARow(dataRecordSession.GetARow(i),dataRecordSession.sessionName);
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
	
	private void CreateTable(String tableType, String[] fields, String tableName) throws SQLException{
		_sqlStatement = new StringBuilder("CREATE " + tableType + " TABLE " + tableName + " (" + tableName + "_id INT UNSIGNED AUTO_INCREMENT,");
		for(int i = 0; i < fields.length; i++){
			_sqlStatement.append(fields[i] + " text,");
		}
		_sqlStatement.append("PRIMARY KEY (" + tableName + "_id))");
		//System.out.println("[Tracing] Create table statement built as: " + _sqlStatement.toString());
		_sqlHelper.Execute(_sqlStatement.toString());
	}
	
	private void InsertARow(String[] records, String tableName) throws SQLException{
		_sqlStatement = new StringBuilder("INSERT INTO " + tableName + _fieldsUnit.toString() + "VALUES (\"");
        for(int i = 0; i < records.length; i++){
        	if(i != (records.length - 1)){
        		_sqlStatement.append(records[i] + "\",\"");
			}else{
				_sqlStatement.append(records[i] + "\")");
			}
        }
        //System.out.println("[Tracing] Insert values statement built as: " + _sqlStatement.toString());
        _sqlHelper.Execute(_sqlStatement.toString());
	}
	
	private StringBuilder BuildFieldUnit(String[] fields){
		StringBuilder fieldsUnit = new StringBuilder("(");
		for(int i = 0; i < fields.length; i++){
			if(i != (fields.length - 1)){
				fieldsUnit.append(fields[i] + ",");
			}else{
				fieldsUnit.append(fields[i] + ")");
			}
		}
		return fieldsUnit;
	}
	
	
	/**
	 * Close the buffer steam used for loading files.
	 * @throws IOException
	 */
	public void Close() throws IOException{
		_SourceData.close();
	}
}
