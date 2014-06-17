#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstdlib>
#include <cstring>

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
  	cout << "Algorithm: File Scan" << endl;
  
	Status status;
	
	// get the heapfile for the relation
	string relName(projNames[0].relName);

	// create or open the heapfile which stores the resulting data	
	HeapFile hf(result, status);
	if(status != OK){
		cerr << "Open heap file for storing the results of heap file scan failed!" << endl;
		return status;
	}

	// opend the heap file 
	// which constructor to use depends on whether it is a filtered scan or not
	HeapFileScan *hfs = NULL;
	if(!attrDesc){
		hfs = new HeapFileScan(relName, status);
	}
	else{
		const int offset = attrDesc->attrOffset;
		const int length = attrDesc->attrLen;
		const Datatype type = static_cast<Datatype>(attrDesc->attrType);
		const char* filter = (char*)attrValue;

		hfs = new HeapFileScan(relName, offset, length, type, filter, op, status);
	}
	
	if(status != OK){
		cerr << "Open heap file for filtered HeapFileScan failed!" << endl;
		delete hfs;
		return status;
	}
		
			
	// scan the whole heap file and get the projection attrs
	// insert the reuslts into the result heap file
	RID outRid;
	RID lastRid;
	Record rec;

	while(hfs->scanNext(outRid, rec) == OK && !(outRid == lastRid)){
		int tempOffset = 0;
  		
		char* result = new char[reclen + 1];
		result[reclen] = '\0';
		
		for(int i = 0; i < projCnt; i ++){
			char* sou = (char*)rec.data + projNames[i].attrOffset;
			char* des = &(result[tempOffset]);
			memcpy(des, sou, projNames[i].attrLen);

			tempOffset += projNames[i].attrLen;			
		}		
			
		lastRid = outRid;
		
		// pack the result into record and insert it into the result hap file		
		RID _outRid;
		Record _rec = {result, reclen};
		status = hf.insertRecord(_rec, _outRid);	

		delete []result;
		result = NULL;	

		if(status != OK){
			delete hfs;
			return status;
		}
	}

	status = hfs->endScan();
	if(status != OK)  return status;
	
	delete hfs;
	return OK;
}
