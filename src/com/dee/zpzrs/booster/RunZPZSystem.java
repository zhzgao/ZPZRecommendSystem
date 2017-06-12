package com.dee.zpzrs.booster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.dee.zpzrs.dal.csv.DataBuilder;
import com.dee.zpzrs.dal.ma.DataConstructor;
import com.dee.zpzrs.dal.mysql.DataLoader;
import com.dee.zpzrs.dal.mysql.MySQLHelper;


public class RunZPZSystem {
	
	public static void main(String[] args){
		// TODO Auto-generated method stub
		/*
		DataBuilder dber = new DataBuilder("data/ratings.csv");
		try {
			dber.BucketGroupDataBy("movieId");
			dber.CloseAllBuffer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		MySQLHelper sqlhp1 = new MySQLHelper("localhost", "3306", "ZPZRecommendSystem", "root", "DZ2175362zhz");
		DataLoader dl1 = new DataLoader("data/ratings.csv", sqlhp1);
		
		try {
			dl1.CSVToMySQL("");
			dl1.Close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		String[] fields = new String[4];
		fields[0] = "userId";
		fields[1] = "movieId";
		fields[2] = "rating";
		fields[3] = "timestamp";
		Map<String, String> ralations = new HashMap<String, String>();
		//ralations.put("userId", "rating");
		/*
		try {
			BufferedReader _OutData = new BufferedReader(new FileReader("data/temp/ratings_66_tmp.csv"));
			DataConstructor dc = new DataConstructor(_OutData.readLine(), fields, "movieId");
			//dc.ConsByRelation(ralations);
			_OutData.close();
			System.out.println("[Trcing]Selected data: " + dc.ConsByRelation(ralations).GetARow(1)[1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "ZPZRecommendSystem", "root", "DZ2175362zhz");
		DataLoader dl = new DataLoader("data/ratings_movieId_s.csv", sqlhp);
		
		try {
			dl.ESVToMySQL("", "movieId", ralations);
			dl.Close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		
	}
	
	public void loadDataToDB(){
		MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "ZPZRecommendSystem_test_1", "root", "DZ2175362zhz");
		DataLoader dl = new DataLoader("data/ratings.csv", sqlhp);
		
		try {
			dl.CSVToMySQL("");
			dl.Close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void groupData(){
		DataBuilder dber = new DataBuilder("data/ratings.csv");
		try {
			dber.BucketGroupDataBy("movieId");
			dber.CloseAllBuffer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
