package com.dee.zpzrs.core;

public abstract class ZPZItem {
	
	private String _itemId;
	
	
	public ZPZItem(String id){
		_itemId = id;
	}
	
	public String GetID(){
		return _itemId;
	}
}
