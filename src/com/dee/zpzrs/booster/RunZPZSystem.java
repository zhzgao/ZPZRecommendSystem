package com.dee.zpzrs.booster;

import java.sql.SQLException;

import com.dee.zpzrs.dal.csv.DataBuilder;
import com.dee.zpzrs.dal.mysql.DataLoader;
import com.dee.zpzrs.dal.mysql.MySQLHelper;


public class RunZPZSystem {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataBuilder dber = new DataBuilder("data/ratings.csv");
		try {
			dber.GroupDataBy("userId");
			
			dber.CloseAllBuffer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void loadDataToDB(){
		MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "ZPZRecommendSystem", "root", "DZ2175362zhz");
		DataLoader dl = new DataLoader("data/ratings.csv", sqlhp);
		
		try {
			dl.CSVToMySQL("");
			dl.Close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
