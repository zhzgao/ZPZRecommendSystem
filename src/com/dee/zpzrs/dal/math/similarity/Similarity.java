package com.dee.zpzrs.dal.math.similarity;

import com.dee.zpzrs.dal.ma.DataSession;

public abstract class Similarity {
	protected DataSession _dataV, _dataU;
	
	public Similarity(DataSession dataV, DataSession dataU){
		_dataV = dataV;
		_dataU = dataU;
	}

}
