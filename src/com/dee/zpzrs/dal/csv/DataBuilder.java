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

	private String _dataFileRootDir, _dataFileName, _dataTempFildDir, _dataResultFileDir;
	private BufferedReader _InputData;
	private BufferedReader _TempData;
	private BufferedWriter _OutData;
	private String[] _fieldsArray;
	private Map<String, Integer> fields, groups;
	
	/**
	 * Use to access csv data file and build new quick access data structure in csv type.
	 * @param dataFileDir the directory of the csv data file.
	 */
	public DataBuilder(String dataFileDir){
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
	
	/**
	 * Group the data by a main field. Use really slow algorithm but cause very low complexity of the file system.
	 * @param fieldName The name of the main field.
	 * @throws Exception
	 */
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
	
	/**
	 * Group the data by main field. Use bucket algorithm to implement the efficiency of grouping process but increase the complexity of file system.
	 * @param fieldName The name of the main field.
	 * @throws Exception
	 */
	public void BucketGroupDataBy(String fieldName) throws Exception{
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_" + fieldName + "_s.csv";
		
		System.out.println("[Tracing]Cleaning temp folder");
		ClearTempFolder();
		
		int fieldIndex = fields.get(fieldName);
		int groupIndexPtr = 1;
		
		String temp;
		String[] tempSplited;
		
		int jobCounter = 0;
		
		while((temp=_InputData.readLine())!= null){
			jobCounter ++;
			tempSplited = temp.split(",");
			System.out.println("[Tracing]Processing job: " + jobCounter + " at " + tempSplited[fieldIndex]);
			if(groups.containsKey(tempSplited[fieldIndex])){
				UpdateBucket(tempSplited, groups.get(tempSplited[fieldIndex]), fieldIndex);
			}else{
				//System.out.println("[Tracing]InserDataAtLast is called.");
				groups.put(tempSplited[fieldIndex], groupIndexPtr);
				groupIndexPtr++;
				CreateBucket(temp, groups.get(tempSplited[fieldIndex]));
			}
		}
		MergeBuckets();
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
						tempSplited[i] = tempSplited[i] + "#" + inValues[i];
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
	
	private void CreateBucket(String record, int bucketId) throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataFileRootDir + "/temp/" + _dataFileName + "_" + bucketId + "_tmp.csv", true);
		//out.write("\n".getBytes());
		out.write(record.getBytes());
		out.close();
	}
	
	private void UpdateBucket(String[] inValues, int bucketId, int fieldIndex) throws Exception{
		_TempData = new BufferedReader(new FileReader(_dataFileRootDir + "/temp/" + _dataFileName + "_" + bucketId + "_tmp.csv"));
		String[] tempSplited = _TempData.readLine().split(",");
		_TempData.close();
		for(int i=0; i<tempSplited.length; i++){
			if(i!=fieldIndex){
				tempSplited[i] = tempSplited[i] + "#" + inValues[i];
			}
		}
		String rebuildRecord = RebuildRecord(tempSplited, ",");
		_OutData = new BufferedWriter(new FileWriter(_dataFileRootDir + "/temp/" + _dataFileName + "_" + bucketId + "_tmp.csv"));
		_OutData.write(rebuildRecord);
		_OutData.close();
	}
	
	private void MergeBuckets() throws Exception{
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		_OutData.write(RebuildRecord(_fieldsArray, ",") + "\n");
		for(Map.Entry<String, Integer> entry : groups.entrySet()){
			_TempData = new BufferedReader(new FileReader(_dataFileRootDir + "/temp/" + _dataFileName + "_" + entry.getValue() + "_tmp.csv"));
			_OutData.write(_TempData.readLine() + "\n");
			_TempData.close();
		}
		_OutData.close();
	}
	
	private void BuildFields() throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataResultFileDir, true);
		//out.write("\n".getBytes());
		out.write(RebuildRecord(_fieldsArray, ",").getBytes());
		out.write("\n".getBytes());
		out.close();
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
	
	/**
	 * Remove all stuff in the temp folder.
	 */
	public void ClearTempFolder(){
		FileOperator fo = new FileOperator();
		fo.CreateDirectory("data/temp");
		fo.DeleteAllFiles("data/temp");
	}
	
	/**
	 * Remember to close the buffer after the data access job.
	 * @throws IOException
	 */
	public void CloseAllBuffer() throws IOException{
		_InputData.close();
	}
}
