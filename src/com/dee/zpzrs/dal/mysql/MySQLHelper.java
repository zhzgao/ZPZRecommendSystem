package com.dee.zpzrs.dal.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLHelper {

	private String _serverUrl;
	private String _port;
	private String _dbName;
	private Statement _dbStatement;
	
	public MySQLHelper(String serverUrl, String port, String dbName, String dbUserId, String dbUserPwd){
		
		_serverUrl = serverUrl;
		_port = port;
		_dbName = dbName;
		
		try {
			Connection dbConn = DriverManager.getConnection("jdbc:mysql://" + _serverUrl + ":" 
																			+ _port + "/"
																			+ _dbName + "?verifyServerCertificate=false&useSSL=true",
																			dbUserId, dbUserPwd);
			_dbStatement = dbConn.createStatement();
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
	}
	
	public boolean Execute(String statement) throws SQLException{
		return _dbStatement.execute(statement);
	}
	
	public ResultSet ExecuteQuery(String statement) throws SQLException{
		return _dbStatement.executeQuery(statement);
	}
	
}
