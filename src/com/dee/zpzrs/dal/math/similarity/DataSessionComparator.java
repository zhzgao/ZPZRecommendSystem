package com.dee.zpzrs.dal.math.similarity;

import java.util.ArrayList;
import java.util.List;

import com.dee.zpzrs.dal.ma.DataSession;

public class DataSessionComparator {
	private DataSession _dataV, _dataU;
	private String _comparedField;
	private List<Integer> _sameRecordIndexCT, _sameRecordIndexSD;
	//private List<List<Integer>> _compareResult;
	private CompareResultSet _compareResult;
	
	public DataSessionComparator(DataSession dataV, DataSession dataU, String comparedField){
		_dataV = dataV;
		_dataU = dataU;
		_comparedField = comparedField;
		_sameRecordIndexCT = new ArrayList<Integer>();
		_sameRecordIndexSD = new ArrayList<Integer>();
	}
	
	public CompareResultSet GetIntersection(){
		if(_dataV.dataRecordSize>=_dataU.dataRecordSize){
			IntersectBy(_dataV, _dataU);
			_compareResult = new CompareResultSet(_sameRecordIndexCT, _sameRecordIndexSD);
		}else{
			IntersectBy(_dataU, _dataV);
			_compareResult = new CompareResultSet(_sameRecordIndexSD, _sameRecordIndexCT);
		}
		return _compareResult;
	}
	
	private void IntersectBy(DataSession dataCT, DataSession dataSD){
		String recordHold;
		for(int i=0; i<dataCT.dataRecordSize; i++){
			recordHold = dataCT.GetARecordKey(_comparedField, i);
			if(dataSD.ContainsRecord(_comparedField, recordHold)){
				_sameRecordIndexCT.add(i);
				_sameRecordIndexSD.add(Integer.parseInt(dataSD.GetARecordValue(_comparedField, recordHold)));
			}
		}
	}

}
