package com.dee.zpzrs.dal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.dee.zpzrs.core.ZPZUser;
import com.dee.zpzrs.dal.agent.SortByRatingScore;

public class User extends ZPZUser {

	private List<Rating> _ratings;
	
	public User(String id){
		super(id);
		_ratings = new ArrayList<Rating>();
	}
	
	public void addRating(Rating rating){
		_ratings.add(rating);
		sortRatings();
		
	}
	
	/**
	 * Check if rating is exist, return the index of the rating in the rating ArrayList if exist, -1 if not.
	 * @param rating Input the rating to be checked for the existence in the ArrayList.
	 */
	public int isRatingExist(Rating rating){
		for(int i = 0; i < _ratings.size(); i++){
			if(_ratings.get(i).equals(rating)) return i;
		}
		return -1;
	}
	
	public void sortRatings(){
		Collections.sort(_ratings, new SortByRatingScore());
	}
}
