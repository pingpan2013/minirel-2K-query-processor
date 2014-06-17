#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>
#include <cassert>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07


/*  
 * Help function used in snl.cpp and sml.cpp:
 * 
 * Projection on the fly and insert the result record into 
 * the result heap file
 * 
 * return OK on success
 *        an error code otherwise
 */
Status Operators::ProjectAndInsert(HeapFile &result,     // Heap file storing reuslts 
			const string &relName1,		 // Relation 1
			const string &relName2,		 // Relation 2
			Record &rec1, 			 // The record for relation 1
			Record &rec2, 			 // The record for relation 2
			const int projCnt, 		 // The # of attrs in projection
			const AttrDesc attrDescArray[],	 // The projection list
			const int reclen)		 // Length of a tuple in the result relation
{	
	Status status;

	void* rec1_data = rec1.data;
	void* rec2_data = rec2.data;
	int tempOffset = 0;
	char* res_data = new char[reclen + 1];
	res_data[reclen] = '\0';

	for(int i = 0; i < projCnt; i ++){
		void* source_data;

		// If the projection attr is in the realtion 1
		if(attrDescArray[i].relName == relName1)
			source_data = rec1_data;
		// Otherwise in the realtion 2
		else{
			assert(attrDescArray[i].relName == relName2);
			source_data = rec2_data;
		}
	
		// Copy the data
		memcpy(res_data + tempOffset,
		       (char*)source_data + attrDescArray[i].attrOffset,
		       attrDescArray[i].attrLen);
		
		tempOffset += attrDescArray[i].attrLen;
	}
	
	// Pack the output relation into record and insert the record into 
	// the resulting heap file
	RID res_rid;
	Record res_rec = {res_data, reclen};

	status = result.insertRecord(res_rec, res_rid);
	if(status != OK) return status;

	delete []res_data;

	return OK;
}

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
	Status status;

	// error check if either of the two join predicates is null
	if(!attr1 || !attr2) return ATTRNOTFOUND;

	// convert the data structure of all the attrs from attrInfo to AttrDesc
	AttrDesc* attr_1 = new AttrDesc;
	status = attrCat->getInfo(attr1->relName, attr1->attrName, *attr_1);
	if(status != OK){
		delete attr_1;
		return status;
	}

	AttrDesc* attr_2 = new AttrDesc;
	status = attrCat->getInfo(attr2->relName, attr2->attrName, *attr_2);
	if(status != OK){
		delete attr_1;
		delete attr_2;
		return status;
	}
	
	int reclen = 0;
	AttrDesc* attr_n = new AttrDesc[projCnt];
	status = Operators::ConvertFromInfoToDesc(projNames, projCnt, attr_n, reclen);	
	if(status != OK){
		delete attr_1;
		delete attr_2;
		delete [] attr_n;
		return status;
	}

	// decide which join algorithm to use according to the index and op
	// The order of PREFERENCE for the algorithms is:
	// 	INL (indexed-nested loops join)   	// guaranteed that the second attr is the indexed one
	//   -> SMJ (sort-merge join) 
	//   -> SNL (simple nested-loops join)
	if(op == EQ && attr_2->indexed == 1){
		status = Operators::INL(result, projCnt, attr_n, *attr_1, op, *attr_2, reclen);	
	}
	else if(op == EQ && attr_2->indexed == 0 && attr_1->indexed == 1){
		status = Operators::INL(result, projCnt, attr_n, *attr_2, op, *attr_1, reclen);	
	}
	else if(op == EQ && attr_1->indexed == 0 && attr_2->indexed == 0){
		status = Operators::SMJ(result, projCnt, attr_n, *attr_1, op, *attr_2, reclen);	
	}
	else{
		status = Operators::SNL(result, projCnt, attr_n, *attr_1, op, *attr_2, reclen);	
	}

	delete attr_1;
	delete attr_2;
	delete [] attr_n;

	return status;
}


// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    	int tmpInt1, tmpInt2;
    	double tmpFloat1, tmpFloat2, floatDiff;

   	// Compare the attribute values using memcpy to avoid byte alignment issues
    	switch(attrDesc1.attrType)
    	{
        	case INTEGER:
            		memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            		memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            		return tmpInt1 - tmpInt2;

        	case DOUBLE:
            		memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            		memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            		floatDiff = tmpFloat1 - tmpFloat2;
            	return (fabs(floatDiff) < DOUBLEERROR) ? 0 : (floatDiff < 0 ? floor(floatDiff) : ceil(floatDiff));

        	case STRING:
            		return strncmp(
                	(char *) outerRec.data + attrDesc1.attrOffset, 
                	(char *) innerRec.data + attrDesc2.attrOffset, 
                	MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    	}

    	return 0;
}
