package com.dee.zpzrs.dal.math.similarity;

import java.util.ArrayList;
import java.util.List;

import com.dee.zpzrs.dal.ma.DataSession;

public class PearsonCorrelationSimilarity extends Similarity{
	
	private List<Double> _dataVItems, _dataUItems;
	private CompareResultSet _dataIntersection;
	private double _avgVItems, _avgUItems;
	
	public PearsonCorrelationSimilarity(DataSession dataV, DataSession dataU){
		super(dataV, dataU);
		_dataVItems = new ArrayList<Double>();
		_dataUItems = new ArrayList<Double>();
		
	}
	
	public double ExecuteSimilarity(String CorrFieldName, String ValueFieldName){
		InitExecution(CorrFieldName, ValueFieldName);
		double weightVU;
		double avgSubstractV, avgSubstractU;
		double itemHolderV, itemHolderU;
		double sumA = 0, sumB = 0, sumC = 0;
		for(int i=0; i<_dataIntersection.size; i++){
			itemHolderV = _dataVItems.get(i);
			itemHolderU = _dataUItems.get(i);
			avgSubstractV = itemHolderV - _avgVItems;
			avgSubstractU = itemHolderU - _avgUItems;
			sumA = sumA + (avgSubstractV * avgSubstractU);
			sumB = sumB + (avgSubstractV * avgSubstractV);
			sumC = sumC + (avgSubstractU * avgSubstractU);
		}
		weightVU = sumA/(Math.sqrt(sumB)*Math.sqrt(sumC));
		return weightVU;
		
	}
	
	private void InitExecution(String CorrFieldName, String ValueFieldName){
		DataSessionComparator dsc = new DataSessionComparator(_dataV, _dataU, CorrFieldName);
		_dataIntersection = dsc.GetIntersection();
		double itemHolderV, itemHolderU;
		double itemSumV = 0, itemSumU = 0;
		double itemsSize = _dataIntersection.size;
		for(int i=0; i<itemsSize; i++){
			itemHolderV = Double.parseDouble(_dataV.GetARecordKey(ValueFieldName, _dataIntersection._sameRecordIndexV.get(i)));
			itemSumV = itemSumV + itemHolderV;
			_dataVItems.add(itemHolderV);
			itemHolderU = Double.parseDouble(_dataU.GetARecordKey(ValueFieldName, _dataIntersection._sameRecordIndexU.get(i)));
			itemSumU = itemSumU + itemHolderU;
			_dataUItems.add(itemHolderU);
		}
		_avgVItems = itemSumV/itemsSize;
		_avgUItems = itemSumU/itemsSize;
	}

}
