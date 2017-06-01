package com.dee.zpzrs.dal;

import java.util.HashMap;
import java.util.Map;

public class DataPool {

	private Map<String, Movie> _moviesPool;
	private Map<String, Director> _directorPool;
	private Map<String, User> _userPool;
	
	public DataPool(){
		_moviesPool = new HashMap<String, Movie>();
		_directorPool = new HashMap<String, Director>();
		_userPool = new HashMap<String, User>();
	}
}
