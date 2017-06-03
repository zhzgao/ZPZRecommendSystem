package com.dee.zpzrs.dal.mysql;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

public class DataLoader {

	private BufferedReader _SourceData;
	private String _fileName;
	private StringBuilder _sqlStatement;
	
	public DataLoader(String dataFileDir){
		try {
			_SourceData = new BufferedReader(new FileReader(dataFileDir));
			_fileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1 ,dataFileDir.lastIndexOf('.'));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void CSVToMySQL(MySQLHelper mysqlHelper) throws IOException, SQLException{
		String record = null;
		String[] fields = null;
		StringBuilder fieldsUnit = new StringBuilder("(");
		
		fields = _SourceData.readLine().split(",");//create table
		_sqlStatement = new StringBuilder("CREATE TABLE " + _fileName + " (" + _fileName + "_id INT UNSIGNED AUTO_INCREMENT,");
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
		
		mysqlHelper.Execute(_sqlStatement.toString());
		
		int jobCounter = 0;
        while ((record = _SourceData.readLine()) != null){//insert data
        	System.out.println("\n\n======= Job " + (jobCounter+1) + " processing =======");
            String[] Elements = record.split(",");
            
            _sqlStatement = new StringBuilder("INSERT INTO " + _fileName + fieldsUnit.toString() + "VALUES (");
            for(int i = 0; i < Elements.length; i++){
            	if(i != (Elements.length - 1)){
            		_sqlStatement.append(Elements[i] + ",");
    			}else{
    				_sqlStatement.append(Elements[i] + ")");
    			}
            }
            mysqlHelper.Execute(_sqlStatement.toString());
            
            jobCounter++;
            System.out.println("[Tracing]Job " + jobCounter + " is done!");
        }
        
	}
	
	public void Close() throws IOException{
		_SourceData.close();
	}
}
