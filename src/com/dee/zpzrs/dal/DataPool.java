package com.dee.zpzrs.dal;

import java.util.HashMap;
import java.util.Map;

import com.dee.zpzrs.core.ZPZAtom;

public class DataPool {

	private Map<String, Map<String, ZPZAtom>> _datapools;
	
	public DataPool(){
		_datapools = new HashMap<String, Map<String, ZPZAtom>>();
	}
	
	public void newPool(String poolName, Map<String, ZPZAtom> pool){
		_datapools.put(poolName, pool);
	}
	
	public boolean isInPool(String poolName, String keyInPool){
		return _datapools.get(poolName).containsKey(keyInPool);
	}
	
	public void addElementToPool(String poolName, String elementKey, ZPZAtom elementValue){
		_datapools.get(poolName).put(elementKey, elementValue);
	}
	
	
}
