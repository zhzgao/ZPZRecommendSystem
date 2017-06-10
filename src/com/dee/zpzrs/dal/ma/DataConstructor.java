package com.dee.zpzrs.dal.ma;

import java.util.HashMap;
import java.util.Map;

public class DataConstructor {

	private Map<String, Map<String, String>> _dataRecordSession;
	private Map<String, String[]> _dataRecordMidSession;
	private Map<String, Integer> _fieldsIndex;
	private int _fieldRecordSize;
	private String[] _fields;
	private String _mainField;
	
	public DataConstructor(String dataRecord, String[] fields, String mainField){
		_fields = fields;
		_mainField = mainField;
		_dataRecordSession = new HashMap<String, Map<String, String>>();
		_dataRecordMidSession = new HashMap<String, String[]>();
		_fieldsIndex = new HashMap<String, Integer>();
		String[] dataFieldRecords = SplitDataBy(",", dataRecord);
		System.out.println("[Tracing]Fields lenth: " + fields.length);
		for(int i=0; i<_fields.length; i++){
			if(!_fields[i].equals(_mainField)){
				_dataRecordSession.put(_fields[i], new HashMap<String, String>());
				_dataRecordMidSession.put(_fields[i], SplitDataBy("#", dataFieldRecords[i]));
				_fieldsIndex.put(_fields[i], i);
			}
		}
	}
	
	public void ConsByRelation(Map<String, String> relations){
		System.out.println("[Tracing]ConsByRelation is called. _dataRecordMidSession size is: " + _dataRecordMidSession.size());
		System.out.println("[Tracing]ConsByRelation is called. _dataRecordMidSession value size is: " + _dataRecordMidSession.get("userId").length);
		System.out.println("[Tracing]ConsByRelation is called. _fieldRecordSize: " + _fieldRecordSize);
		
		for(int i=0; i<_fieldRecordSize; i++){
			for(Map.Entry<String, String[]> entry : _dataRecordMidSession.entrySet()){
				//System.out.println("[Tracing]Entry key is: " + entry.getKey());
				if(relations.containsKey(entry.getKey())){
					_dataRecordSession.get(entry.getKey()).put(entry.getValue()[i], _dataRecordMidSession.get(relations.get(entry.getKey()))[i]);
				}else{
					_dataRecordSession.get(entry.getKey()).put(entry.getValue()[i], entry.getValue()[i]);
				}
			}
		}
		
	}
	
	private String[] SplitDataBy(String separator, String dataRecord){
		String[] out;
		if(dataRecord.contains(separator)){
			out = dataRecord.split(separator);
			_fieldRecordSize = out.length;
			//System.out.println("[Tracing]SplitDataBy is called. _fieldRecordSize: " + _fieldRecordSize);
			return out;
		}else{
			out = new String[1];
			_fieldRecordSize = 1;
			out[0] = dataRecord;
			return out;
		}
	}
	
	public Map<String, Map<String, String>> GetDataRecordMap(){
		return _dataRecordSession;
	}
}
