#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>
#include <iostream>

using namespace std;

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

#define NDEBUG

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
	Status status;

	// get the attr information about the realtion 
	int aCnt;
	AttrDesc *attrs = new AttrDesc[attrCnt];
	status = attrCat->getRelInfo(relation, aCnt, attrs);
	
	// error checking 	
	if(status != OK || aCnt != attrCnt){
		delete []attrs;
		if(status != OK){
			Error::print(status);
			return status;
		}
		else 
			return ATTRTYPEMISMATCH;
	}	

	// calculate the total length of the record
	// then create a buffer according to the size
	int rLength = 0;
	for(int i = 0; i < attrCnt; i ++){
		rLength += attrs[i].attrLen;
	}

	char *rBuffer = new char[rLength + 1];
	rBuffer[rLength] = '\0';
	
	// match the attr information in the attrList[]
	// then append the attr data into the record buffer
	for(int i = 0; i < attrCnt; i ++){
		int attr_index = 0;
		while(strcmp(attrList[attr_index].attrName, attrs[i].attrName))  attr_index++;
		
		// return if we cannot match the attr
		// return if the attribute value is NULL
		if(attr_index == attrCnt || attrList[i].attrValue == NULL){
			delete []attrs;
			delete []rBuffer;
			return ATTRTYPEMISMATCH;
		}	
		
		void* des = &rBuffer[attrs[i].attrOffset];
		void* sou = attrList[attr_index].attrValue;
		memcpy(des, sou, attrs[i].attrLen);
	}	

#ifndef NDEBUG
	string str(rBuffer);
	cout << "The content of the record is: " << str << endl; 
	cout << "With the length: " << rLength << endl;
#endif

	// pack the record information
	// insert record on the existing heap files
	// if not exist, create a new heap file for the relation	
	Record newR = {rBuffer, rLength};

	HeapFile newHF(relation, status);
	if(status != OK){
		Error::print(status);
		delete []attrs;
		delete []rBuffer;
		return status;
	}	
	
	RID newRid;
	status = newHF.insertRecord(newR, newRid);
	if(status != OK){
		Error::print(status);
		delete []attrs;
		delete []rBuffer;
		return status;
	}	

	// if there are indexes for the relation
	// insert the record ID into each index for the relation 
	for(int i = 0; i < attrCnt; i ++){
		if(attrs[i].indexed == 1){
			const int offset = attrs[i].attrOffset;
			const int length = attrs[i].attrLen;
			const Datatype type = static_cast<Datatype>(attrs[i].attrType);
			const int unique = 0;

			Index index(relation, offset, length, type, unique, status);
			if(status != OK){
				Error::print(status);
				delete []attrs;
				delete []rBuffer;
				return status;
			}	
			
			// insert the entry into the index
			char tempStr[length];
			void* value = tempStr;
			memcpy(value, rBuffer + offset, length);
			status = index.insertEntry(value, newRid);
			if(status != OK){
				Error::print(status);
				delete []attrs;
				delete []rBuffer;
				return status;
			}
		}
	}	

	delete []attrs;
	delete []rBuffer;		
	return status;
}



