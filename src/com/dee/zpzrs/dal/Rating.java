package com.dee.zpzrs.dal;

import java.sql.Timestamp;

import com.dee.zpzrs.core.ZPZItem;

public class Rating extends ZPZItem{

	private String _userId;
	private String _movieId;
	public float score;
	private Timestamp _timestamp;
	
	public Rating(String id, String userId, String movieId){
		super(id);
		_userId = userId;
		_movieId = movieId;
	}
	
	public void setTime(long timestamp){
		_timestamp = new Timestamp(timestamp);
	}
	
	public String getUserId(){
		return _userId;
	}
	
	public String getMovieId(){
		return _movieId;
	}
	
	public Timestamp getTime(){
		return _timestamp;
	}
}
