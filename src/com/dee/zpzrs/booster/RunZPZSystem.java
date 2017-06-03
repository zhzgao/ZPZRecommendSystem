package com.dee.zpzrs.booster;

import java.sql.Timestamp;

import com.dee.zpzrs.dal.mysql.DataLoader;
import com.dee.zpzrs.dal.mysql.MySQLHelper;


public class RunZPZSystem {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Timestamp tm = new Timestamp(1112486027);
		System.out.println(tm);
		
		DataLoader dl = new DataLoader("data/ratings.csv");
		MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "ZPZRecommendSystem", "root", "DZ2175362zhz");
		
		try {
			dl.CSVToMySQL(sqlhp);
			dl.Close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
