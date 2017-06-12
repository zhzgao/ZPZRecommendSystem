package com.dee.zpzrs.dal.ma;

import java.util.HashMap;
import java.util.Map;

public class DataSession {

	private Map<String, Map<String, String>> _dataRecordSession;
	private Map<String, String[]> _dataRecordMidSession;
	private Map<String, Integer> _fieldsIndex;
	private String[] _fields;
	
	public String sessionName;
	public int fieldSize, dataRecordSize;
	
	public DataSession(String sessionName){
		_dataRecordSession = new HashMap<String, Map<String, String>>();
		_dataRecordMidSession = new HashMap<String, String[]>();
		_fieldsIndex = new HashMap<String, Integer>();
		this.sessionName = sessionName;
		fieldSize = 0;
		dataRecordSize = 0;
	}
	
	public void LoadData(Map<String, Map<String, String>> dataRecordSession, 
						 Map<String, String[]> dataRecordMidSession, 
						 Map<String, Integer> fieldsIndex, 
						 String[] fields,
						 int dataRecordSize){
		
		_dataRecordSession = dataRecordSession;
		_dataRecordMidSession = dataRecordMidSession;
		_fieldsIndex = fieldsIndex;
		_fields = fields;
		fieldSize = _dataRecordSession.size();
		this.dataRecordSize = dataRecordSize;
	}
	
	public String GetARecord(String fieldKey, String recordKey){
		return _dataRecordSession.get(fieldKey).get(recordKey);
	}
	
	public String[] GetFields(){
		return _fields;
	}
	
	public String[] GetARow(int rowIndex){
		String[] rowRecords = new String[fieldSize];
		for(int i=0; i<_fields.length; i++){
			rowRecords[i] = _dataRecordMidSession.get(_fields[i])[rowIndex];
		}
		return rowRecords;
	}
	
}
