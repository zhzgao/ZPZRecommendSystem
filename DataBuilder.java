package com.dee.zpzrs.dal.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class DataBuilder {

	private String _dataFileDir, _dataFileRootDir, _dataFileName, _dataTempFildDir, _dataResultFileDir;
	private BufferedReader _InputData;
	private BufferedReader _TempData;
	private BufferedWriter _OutData;
	private String[] _fieldsArray;
	private Map<String, Integer> fields, groups;
	
	public DataBuilder(String dataFileDir){
		_dataFileDir = dataFileDir;
		_dataFileRootDir = dataFileDir.substring(0, dataFileDir.lastIndexOf('/')+1);
		_dataFileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1, dataFileDir.lastIndexOf('.'));
		
		
		//System.out.println("[Tracing] data directory: " + dataFileDir);
		try {
			_InputData = new BufferedReader(new FileReader(dataFileDir));
			fields = new HashMap<String, Integer>();
			groups = new HashMap<String, Integer>();
			_fieldsArray = _InputData.readLine().split(",");
			MapFields();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void GroupDataBy(String fieldName) throws Exception{
		
		_dataTempFildDir = _dataFileRootDir + _dataFileName + "_" + fieldName + "_tmp.csv";
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_" + fieldName + "_s.csv";
		//#Check existence of result file and temporary file.
		//#Delect files if exist.
		File fileTemp=new File(_dataTempFildDir);
		File fileResult=new File(_dataResultFileDir);
		if(fileTemp.exists()) fileTemp.delete();
		if(fileResult.exists()) fileResult.delete();
		
		int fieldIndex = fields.get(fieldName);
		//System.out.println("[Tracing] " + fieldName + " index: " + fieldIndex);
		int groupIndexPtr = 1;
		
		String temp;
		String[] tempSplited;
		
		int jobCounter = 0;
		BuildFields();
		while((temp=_InputData.readLine())!= null){
			jobCounter ++;
			tempSplited = temp.split(",");
			System.out.println("[Tracing]Processing job: " + jobCounter + " at " + tempSplited[fieldIndex]);
			if(groups.containsKey(tempSplited[fieldIndex])){
				InsertData(tempSplited, groups.get(tempSplited[fieldIndex]), fieldIndex);
			}else{
				//System.out.println("[Tracing]InserDataAtLast is called.");
				groups.put(tempSplited[fieldIndex], groupIndexPtr);
				groupIndexPtr++;
				InsertDataAtLast(temp);
			}
		}
		
	}
	
	private void InsertDataAtLast(String record) throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataResultFileDir, true);
		//out.write("\n".getBytes());
		out.write(record.getBytes());
		out.write("\n".getBytes());
		out.close();
		UpdateTempData();
	}
	
	private void InsertData(String[] inValues, int recordIndex, int fieldIndex) throws Exception{
		//System.out.println("[Tracing]InsertData is called.");
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		_TempData = new BufferedReader(new FileReader(_dataTempFildDir));
		String temp, rebuildRecord;
		String[] tempSplited;
		int linePtr = 0;
		while((temp = _TempData.readLine())!=null){
			if(linePtr!=recordIndex){
				//System.out.println("[Tracing]Not target record. RecordIndex is:" + recordIndex);
				_OutData.write(temp + "\n");
				linePtr++;
			}else{
				//System.out.println("[Tracing]Target record.");
				tempSplited = temp.split(",");
				for(int i=0; i<tempSplited.length; i++){
					if(i!=fieldIndex){
						tempSplited[i] = tempSplited[i] + "|" + inValues[i];
					}
				}
				rebuildRecord = RebuildRecord(tempSplited, ",");
				//System.out.println("[Tracing]Rebuid record: " + rebuildRecord);
				_OutData.write(rebuildRecord + "\n");
				linePtr++;
			}
		}
		_OutData.close();
		_TempData.close();
		UpdateTempData();
	}
	
	private void BuildFields() throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataResultFileDir, true);
		//out.write("\n".getBytes());
		out.write(RebuildRecord(_fieldsArray, ",").getBytes());
		out.write("\n".getBytes());
		out.close();
		UpdateTempData();
	}
	
	private void MapFields(){
		int index = 0;
		for(String field : _fieldsArray){
			System.out.println("[Tracing] Map field " + field + " with index: " + index);
			fields.put(field, index);
			index ++;
		}
	}
	
	private String RebuildRecord(String[] strs, String separator){
		String record = strs[0];
		for(int i=1; i<strs.length; i++){
			record = record + separator + strs[i];
		}
		return record;
	}
	
	private void UpdateTempData() throws Exception{
		FileInputStream in = null;
		FileOutputStream out = null;
		
		in = new FileInputStream(_dataResultFileDir);
		out = new FileOutputStream(_dataTempFildDir);
		
		FileChannel fcIn = in.getChannel();  
        FileChannel fcOut = out.getChannel();  
        fcIn.transferTo(0, fcIn.size(), fcOut);
        
		in.close();
		out.close();
		fcIn.close();
		fcOut.close();
	}
	
	public void CloseAllBuffer() throws IOException{
		_InputData.close();
	}
}
