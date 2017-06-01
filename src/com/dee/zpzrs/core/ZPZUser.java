package com.dee.zpzrs.core;

public abstract class ZPZUser {

	private String _userId;
	
	public ZPZUser(String id){
		_userId = id;
	}
	
	public String GetID(){
		return _userId;
	}
}
