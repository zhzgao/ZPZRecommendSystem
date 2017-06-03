package com.dee.zpzrs.dal.agent;

import java.util.Comparator;

import com.dee.zpzrs.dal.Rating;

public class SortByRatingScore implements Comparator<Rating>{

	public int compare(Rating rt1, Rating rt2){
		return (rt1.score > rt2.score)?1:0;
	}
}
