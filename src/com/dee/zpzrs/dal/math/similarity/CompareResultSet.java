package com.dee.zpzrs.dal.math.similarity;

import java.util.List;

public class CompareResultSet {

	public List<Integer> _sameRecordIndexV, _sameRecordIndexU;
	
	public int size;
	
	public CompareResultSet(List<Integer> sameRecordIndexV, List<Integer> sameRecordIndexU){
		_sameRecordIndexV = sameRecordIndexV;
		_sameRecordIndexU = sameRecordIndexU;
		if(_sameRecordIndexV.size() == _sameRecordIndexU.size()){
			size = _sameRecordIndexV.size();
		}else{
			size = 0;
			System.out.println("[WARN]CompareResultSet V and U have different size!");
		}
		
	}
}
