#include "catalog.h"
#include "query.h"
#include "index.h"

/* 
 * Help function:
 * 	convert the data structures of an array of attrs from attrInfo to AttrDesc
 * 
 * Return:
 * 	OK if success
 *	Error code otherwise
 */
Status Operators::ConvertFromInfoToDesc(const attrInfo* projNames,   // Projection list
					const int projCnt,	     // # of attrs in the projection list
					AttrDesc* proj_n,	     // Projection list after conversion
					int &reclen)		     // The length of output relation
{
	Status status;

	for(int i = 0; i < projCnt; i ++){
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, proj_n[i]);
		if(status != OK) return status;
	
		reclen += proj_n[i].attrLen; 
	}

	return OK;
}



/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,          // predicate operation
		         const void *attrValue)      // literal value in the predicate
{	
	Status status;
	AttrDesc* relAttrs = NULL;
	
	// if it's conditional selection
	if(attr){
		relAttrs = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *relAttrs);
		if(status != OK){
			delete relAttrs;
			return status;
		}
	}
	
	// calculate the length of a tuple
	// and convert the datastructures from attrInfo to attrDesc
	int reclen = 0;	
	AttrDesc* proj_n = new AttrDesc[projCnt];
	for(int i = 0; i < projCnt; i ++){
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, proj_n[i]);
		reclen += proj_n[i].attrLen;      // This is the right method to calculate the length

		if(status != OK){
			if(relAttrs){ 
				delete relAttrs;   // take care: you cannot delete a null pointer
			}
			delete []proj_n;           
			return status;
		}
	}	

	// if it meets the two requirements of index select:
	// 1. the attribute in the predicate is indexed
	// 2. operation is Equality
	if(relAttrs && relAttrs->indexed == 1 && op == EQ){
		status = Operators::IndexSelect(result, projCnt, proj_n, relAttrs, op, attrValue, reclen);
	}
	// otherwise, just use scan select
	else{
		status = Operators::ScanSelect(result, projCnt, proj_n, relAttrs, op, attrValue, reclen);
	}	
	
	if(relAttrs) delete relAttrs;
	delete []proj_n;
	
	return status;
}


