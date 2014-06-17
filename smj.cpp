#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cstring>
#include <cassert>

/*
 * Help Function 1: 
 * 
 * Calculate the lenght in bytes of a tuple in the given relation
 * Return the resulting length in bytes
 *	
 */
int calTupleLength(const AttrDesc &attrDesc)
{
	Status status;
	
	// First get the # of the attrs
	RelDesc relDesc;
	status = relCat->getInfo(attrDesc.relName, relDesc);
	if(status != OK)   return status;
	int attrCnt = relDesc.attrCnt;

	// Then get all the attrs information
	AttrDesc *attrs = new AttrDesc[attrCnt];
	status = attrCat->getRelInfo(attrDesc.relName, attrCnt, attrs);
	if(status != OK){
		delete []attrs;
		return status;
	}

	// Calculate the length of the tuple
	int tupleLen = 0;
	for(int i = 0; i < attrCnt; i ++){
		tupleLen += attrs[i].attrLen;
	}

	return tupleLen;
}


/* 
 * Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations 
 */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  	cout << "Algorithm: SM Join" << endl;
	
	Status status;

	// Open the heap file for storing the resulting record
	HeapFile result_hf(result, status);
	if(status != OK)	return status;
	
	// Step 1: Create SortedFile object used to sort the relation 1 on the given left attribute	
	unsigned int ava_pages_num = bufMgr->numUnpinnedPages() * 0.8;	
	int left_rel_length = calTupleLength(attrDesc1);
	int left_max_tuples = ava_pages_num * PAGESIZE / left_rel_length;
	Datatype left_type = static_cast<Datatype>(attrDesc1.attrType);
	
	SortedFile sf_left(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, left_type, left_max_tuples, status);           
	if(status != OK){ return status;  }
	
	// Step 1: Create SortedFile object used to sort the relation 2 on the given right attribute	
	ava_pages_num = bufMgr->numUnpinnedPages() * 0.8;	
	int right_rel_length = calTupleLength(attrDesc2);
	int right_max_tuples = ava_pages_num * PAGESIZE / right_rel_length;
	Datatype right_type = static_cast<Datatype>(attrDesc2.attrType);
	
	SortedFile sf_right(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, right_type, right_max_tuples, status);           
	if(status != OK){ return status;  }

	// Step 2: Merge the sorted files
	Record rec1, rec2;
	Status status1, status2;
	
	status1 = sf_left.next(rec1);
	status2 = sf_right.next(rec2);
	while(status1 == OK && status2 == OK){
		int diff = Operators::matchRec(rec1, rec2, attrDesc1, attrDesc2);
		if(diff <0 ){
			status1 = sf_left.next(rec1);
		}
		else if(diff > 0){
			status2 = sf_right.next(rec2);
		}
		// If match, set a mark on the current rec2.
		// Then fix rec1 and keep scaning the next record in relation 2 until it doesn't match current rec1 
		// After that, move on rec1 and check to see if needed to go back to the last mark in relation 2
		else {
			status2 = sf_right.setMark();
			if(status2 != OK) return status2;

			// The last marked record;
			Record last_marked_rec;
			last_marked_rec.data = rec2.data;
			last_marked_rec.length = rec2.length;

			while(status2 == OK && diff == 0){
				// projection and insert the result record into the result heap file on the fly 
				status = ProjectAndInsert(result_hf, attrDesc1.relName, attrDesc2.relName,
							             rec1, rec2, projCnt, attrDescArray, reclen);

				if(status != OK)  return status;

				status2 = sf_right.next(rec2);
				diff = Operators::matchRec(rec1, rec2, attrDesc1, attrDesc2);
			}
			
			// Take Care: You may reach the end of file 2 but you don't want to
			// return an error code
			if(status2 != FILEEOF && status2 != OK)   return status2;

			// When finding a record in relation 2 that doesn't match rid1 on the relation 1,
			// Move on rec1 on the relation 1 to see if the next rec1 matches the last mark in relation 2
			// If it matches, relation 2 goes back to the last mark
			status1 = sf_left.next(rec1);
			while (status1 == OK && Operators::matchRec(rec1, last_marked_rec, attrDesc1, attrDesc2) == 0) {
				status2 = sf_right.gotoMark();	
				status2 = sf_right.next(rec2);
				
				diff = Operators::matchRec(rec1, rec2, attrDesc1, attrDesc2);
				
				// For each duplicate tup in file 1, just run another "while"
				while(status2 == OK && diff == 0){
					// projection and insert the result record into the result heap file on the fly 
					status = ProjectAndInsert(result_hf, attrDesc1.relName, attrDesc2.relName,
							        	     rec1, rec2, projCnt, attrDescArray, reclen);

					if(status != OK)  return status;

					status2 = sf_right.next(rec2);
					diff = Operators::matchRec(rec1, rec2, attrDesc1, attrDesc2);
				}
				if ( status2 != FILEEOF && status2 != OK ) return status2;
				
				status1 = sf_left.next(rec1);
			}
			if ( status1 != FILEEOF && status1 != OK ) return status1;
		}
	}

  	return OK;
}

