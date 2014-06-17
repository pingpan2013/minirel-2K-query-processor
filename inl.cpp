#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cassert>
#include <cstring>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  	cout << "Algorithm: Indexed NL Join" << endl;

	Status status;
	string relName1 = attrDesc1.relName;
	string relName2 = attrDesc2.relName;
	
	// open the heap file for storing the resulting data
	HeapFile result_hf(result, status);
	if(status != OK){
		cerr << "Open heap file for storing the results of INL join failed!" << endl;
		return status;
	}
	
	// open the heap file for the outer relation
	HeapFileScan hfs(relName1, status);
	if(status != OK)   { 	return status;	}
	
	// Indexed-nested loops join: R is the outer relation, 
	//                            S is the inner relation
	// 
	// Algorithm:
	// for each tuple r in R do                  
	// 	Probe Index on S
	//		for each matching tuple s do add <r, s> to the result heap file
	RID rid1, rid2;
	Record rec1, rec2;
	
	// loop through the outer relation
	while(hfs.scanNext(rid1, rec1) == OK){
		//first get the desired attrbute value		
	    	void* rec1_data = rec1.data;
		int attrLen = attrDesc1.attrLen;
		char* attrValue = new char[attrLen + 1];
		attrValue[attrLen] = '\0';

		memcpy(attrValue, (char*)rec1_data + attrDesc1.attrOffset, attrLen);
		
		// Create anohter HeapFileScan object for the inner relation, 
		// We need use this object to access the getRandomRecord() function to get the record data
		HeapFileScan inner_hfs(relName2, status);
		if(status != OK) {	return status;   }

		// open the indexed file for the inner relation
		const Datatype type = static_cast<Datatype>(attrDesc2.attrType);
		Index iscan(relName2, attrDesc2.attrOffset, attrDesc2.attrLen, type, 0, status);
		if(status != OK)   { 	return status;	}

		// loop through the inner relation
		status = iscan.startScan((void*)attrValue);			
		if(status != OK) return status;
		while(iscan.scanNext(rid2) == OK){
			inner_hfs.getRandomRecord(rid2, rec2);
					
			status = ProjectAndInsert(result_hf, relName1, relName2, rec1, rec2, projCnt, attrDescArray, reclen);
			if(status != OK)   return status;
		}
		
		status = inner_hfs.endScan();
		if(status != OK)  {   return status;    }
			
		status = iscan.endScan();
		if(status != OK)  {   return status;    }
	}
	
	status = hfs.endScan();
  	return status;
}

