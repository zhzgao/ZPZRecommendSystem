package com.dee.zpzrs.dal.ma;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

public class DataSession {

	public Map<String, Map<String, String>> _dataRecordSession;
	private Map<String, String[]> _dataRecordMidSession;
	private Map<String, Integer> _fieldsIndex;
	private String[] _fields;
	private int _dataRecordMidSessionBottom;
	
	public String sessionName;
	public int fieldSize, dataRecordSize;
	
	public DataSession(String sessionName){
		_dataRecordSession = new HashMap<String, Map<String, String>>();
		_dataRecordMidSession = new HashMap<String, String[]>();
		_fieldsIndex = new HashMap<String, Integer>();
		_dataRecordMidSessionBottom = 0;
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
		_dataRecordMidSessionBottom = dataRecordSize-1;
		fieldSize = _dataRecordSession.size();
		this.dataRecordSize = dataRecordSize;
	}
	
	public boolean ContainsRecord(String fieldKey, String recordKey){
		return _dataRecordSession.get(fieldKey).containsKey(recordKey);
	}
	
	public String GetARecordValue(String fieldKey, String recordKey){
		return _dataRecordSession.get(fieldKey).get(recordKey);
	}
	
	public String GetARecordKey(String fieldKey, int recordIndex){
		return _dataRecordMidSession.get(fieldKey)[recordIndex];
	}
	
	public String[] GetFields(){
		return _fields;
	}
	
	public int GetFiledsIndex(String FieldName){
		return _fieldsIndex.get(FieldName);
	}
	
	public String[] GetARow(int rowIndex){
		String[] rowRecords = new String[fieldSize];
		for(int i=0; i<_fields.length; i++){
			rowRecords[i] = _dataRecordMidSession.get(_fields[i])[rowIndex];
		}
		return rowRecords;
	}
	
	/**
	 * Build fields for the data session.
	 * @param fields An array hold all the fields name with the order of the array elements.
	 * @param fieldRecordsSize The scale of the records the data session will contain.
	 */
	public void BuildFields(String[] fields, int fieldRecordsSize){
		dataRecordSize = fieldRecordsSize;
		_fields = fields;
		fieldSize = fields.length;
		for(int i=0; i<_fields.length; i++){
			_fieldsIndex.put(_fields[i], i);
			_dataRecordSession.put(_fields[i], new IdentityHashMap<String, String>());
			_dataRecordMidSession.put(_fields[i], new String[dataRecordSize]);
		}
	}
	
	/**
	 * Set a row of records to the bottom of data session
	 * @param rowRecords An array hold all record of a row in the data session.
	 */
	public void SetARow(String[] rowRecords){
		if(_dataRecordMidSessionBottom != this.dataRecordSize-1){
			for(int i=0; i<rowRecords.length; i++){
				_dataRecordSession.get(_fields[i]).put(new String(rowRecords[i]), String.valueOf(i));
				_dataRecordMidSession.get(_fields[i])[_dataRecordMidSessionBottom] = rowRecords[i];
			}
			_dataRecordMidSessionBottom ++;
		}else{
			System.out.println("[WARN]DataSession has reached the bottom! Row set failed!");
		}
		
	}
	
	
}
