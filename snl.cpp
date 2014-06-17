#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cassert>
#include <cstring>
#include <iostream>

/* 
 * Simple nested-loops joins with attrDesc1 as the outer relation 
 * and attrDesc2 as the inner relation
 */

Status Operators::SNL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The right attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  	cout << "Algorithm: Simple NL Join" << endl;

	Status status;
	string relName1 = attrDesc1.relName;
	string relName2 = attrDesc2.relName;
	
	// open the heap file for storing the resulting data
	HeapFile result_hf(result, status);
	if(status != OK){
		cerr << "Open heap file for storing the results of SNL join failed!" << endl;
		return status;
	}
	
	// open the heap file for the outer relation
	HeapFileScan hfs(relName2, status);
	if(status != OK)   { 	return status;	}
	
	// Simple nested-loops join:  R is the outer relation, 
	//                            S is the inner relation
	// Algorithm:
	// for each tuple r in R do                  
	// 	for each tuple s in S 
	//		for each matching tuple s do add <r, s> to the result heap file
	RID rid1, rid2;
	Record rec1, rec2;
	
	// loop through the outer relation
	while(hfs.scanNext(rid2, rec2) == OK){
		//first get the value of the attribute used to match  	
	    	void* rec2_data = rec2.data;
		int   attrLen = attrDesc2.attrLen;
		char* attrValue = new char[attrLen + 1];
		attrValue[attrLen] = '\0';
		memcpy(attrValue, (char*)rec2_data + attrDesc2.attrOffset, attrLen);

		// open the heap file for the inner relation
		const Datatype type = static_cast<Datatype>(attrDesc2.attrType);
		HeapFileScan inner_hfs(relName1, attrDesc1.attrOffset, attrDesc1.attrLen, type, attrValue, op, status);
		if(status != OK)   { 	return status;	}

		// Loop through the inner relation to test
		// if the current record matches
		while(inner_hfs.scanNext(rid1, rec1) == OK){
			status = ProjectAndInsert(result_hf, relName1, relName2, rec1, rec2, projCnt, attrDescArray, reclen);
			if(status != OK)	return status;	
		}

		status = inner_hfs.endScan();
		if(status != OK)    return status;
	}

	status = hfs.endScan();
  	return status;
}

