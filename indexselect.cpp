#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>


Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
  	cout << "Algorithm: Index Select" << endl;

	Status status;
	
	// create open the heap file which stores the resulting data
	HeapFile hf(result, status);
	if(status != OK) {
		cerr << "Open heap file for storing the results of the index scan failed!" << endl;
		return status;
	}

	// open the index file 	
	string relName(projNames[0].relName);
	const int offset = attrDesc->attrOffset;
	const int length = attrDesc->attrLen;
	const Datatype type = static_cast<Datatype>(attrDesc->attrType);
	const int unique = 0;
	
	Index iscan(relName, offset, length, type, unique, status);
	if(status != OK){
		return status;
	}

	// start the index scan 
	// insert the results into the opened result heap file
	HeapFileScan hfs(relName, status);   // we need the hfs object to access the getRecord() function
	if(status != OK) return status;

	RID outRid;
	Record rec;

	status = iscan.startScan(attrValue);
	while(iscan.scanNext(outRid) == OK){
		hfs.getRandomRecord(outRid, rec);
	
		int tempOffset = 0;

		char* result = new char[reclen + 1];
		result[reclen] = '\0';

		for(int i = 0; i < projCnt; i ++){
			char* sou = (char*)rec.data + projNames[i].attrOffset;
			char* des = &(result[tempOffset]);
			memcpy(des, sou, projNames[i].attrLen);
		
			tempOffset += projNames[i].attrLen;
		}

		RID _outRid;
		Record _rec = {result, reclen};
		status = hf.insertRecord(_rec, _outRid);

		delete []result;
		result = NULL;

		if(status != OK){
			return status;
		}
	}
	
	status = iscan.endScan();
  	return status;
}
