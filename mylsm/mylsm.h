#include "access/nbtree.h"
typedef struct
{
	Oid relTable;
	Oid lev0;
	bool lev0full;
    
	Oid lev1;
	int num_tup;
	int num_tup0;
	Oid user_info;
	Oid db_info;
	
} mylsmInfo;
