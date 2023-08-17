#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "access/nbtree.h"
#include "commands/vacuum.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"

#include "mylsm.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(mylsm_handler);

BTMetaPageData* pgBtMeta;
Buffer buf;
Page pg;

BTMetaPageData* get_buffer_page(Relation relTableIndex)
{
    buf = _bt_getbuf(relTableIndex, BTREE_METAPAGE, BT_WRITE);
    pg = BufferGetPage(buf);
    //BTMetaPageData* pgBtMeta;
    pgBtMeta = BTPageGetMeta(pg);
    return pgBtMeta;
}

void release_buffer_page(Relation relTableIndex,Buffer buf)
{
    _bt_relbuf(relTableIndex, buf);
}

static void mylsmInit(mylsmInfo* lsmInfoPtr, Relation relTableIndex, int n_items)
{	
	elog(LOG, "Initializing LSM Parameters");
	lsmInfoPtr -> relTable = relTableIndex -> rd_index -> indrelid;
	lsmInfoPtr -> lev0 = relTableIndex -> rd_id;
	lsmInfoPtr -> lev1 = InvalidOid;
	lsmInfoPtr -> lev0full = false;

	lsmInfoPtr -> num_tup = n_items;
	lsmInfoPtr -> num_tup0 = 0;
	lsmInfoPtr -> db_info = MyDatabaseId;
	lsmInfoPtr -> user_info = GetUserId();
	elog(LOG, "LSM Initialization Complete");
}

IndexBuildResult* mylsmBuildIndexLev0(Relation relationTable, Relation relationTableIndex, IndexInfo *relTableIndexInfo)
{
    elog(LOG,"Building myLSM Level 0 index");

    Oid OriginalAm;
    OriginalAm = relationTableIndex -> rd_rel -> relam;
    relationTableIndex -> rd_rel -> relam =  BTREE_AM_OID;
    IndexBuildResult* output;
    output = btbuild(relationTable, relationTableIndex, relTableIndexInfo);
    relationTableIndex -> rd_rel -> relam =  OriginalAm;

    pgBtMeta = get_buffer_page(relationTableIndex);
    mylsmInfo* pgMyLsmInfo;
    pgMyLsmInfo = (mylsmInfo*)(pgBtMeta+1);

    mylsmInit(pgMyLsmInfo, relationTableIndex, output -> index_tuples);
    release_buffer_page(relationTableIndex,buf);
    return output;
}

void mylsmBuildIndexLev1(Relation relTable, Relation relTableIndex, mylsmInfo* lsmInfoPtr){
    
    char* relName;
    relName = relTableIndex -> rd_rel -> relname.data;
    
    char* l1Name;
    l1Name = psprintf("%s_lev1", relName);
    
    lsmInfoPtr -> lev1 = index_concurrently_create_copy(relTable, relTableIndex -> rd_id,false ,l1Name);
    
    Relation lev1;
    lev1 = index_open(lsmInfoPtr -> lev1, AccessShareLock);
    index_build(relTable, lev1, BuildIndexInfo(lev1), false, false);
    index_close(lev1, AccessShareLock);

}


static void mylsmMerge(Oid l0Oid, Oid relTableOid, Oid l1Oid)
{
    elog(LOG,"Transfering of  Level 0 to Level 1");
    Relation l0 = index_open(l0Oid, 3);
    Relation relTable = table_open(relTableOid, AccessShareLock);
    Relation l1 = index_open(l1Oid, 3);

    Oid tempAm;
    tempAm = l1 -> rd_rel -> relam;
    l1 -> rd_rel -> relam = BTREE_AM_OID;
    IndexScanDesc scan;
    scan = index_beginscan(relTable, l0, SnapshotAny, 0, 0);
    scan -> xs_want_itup = true;
    btrescan(scan, NULL, 0, 0, 0);

    bool items;

    for (items = _bt_first(scan, ForwardScanDirection); items; items = _bt_next(scan, ForwardScanDirection))
    {
        IndexTuple iTup = scan -> xs_itup;
        if (BTreeTupleIsPosting(iTup))
        {
            ItemPointerData saveTid = iTup -> t_tid;
            unsigned short saveInfo = iTup -> t_info;
            iTup -> t_info = (saveInfo & ~(INDEX_SIZE_MASK | INDEX_ALT_TID_MASK)) + BTreeTupleGetPostingOffset(iTup);
            iTup -> t_tid = scan -> xs_heaptid;
            _bt_doinsert(l1, iTup, false, false, relTable);
            iTup -> t_tid = saveTid;
            iTup -> t_info = saveInfo;
        }
        else
            _bt_doinsert(l1, iTup, false, false, relTable);
    }

    index_endscan(scan);
    
    l1 -> rd_rel -> relam = tempAm;
    index_close(l0, 3);
    table_close(relTable, AccessShareLock);
    index_close(l1, 3);
    elog(LOG, "Merge completed");
}

static void mylsmTruncate(Oid relTableIndexOid, Oid relTableOid)
{
	elog(LOG, "Clearing of Level 0 started");
	Relation relTableIndex = index_open(relTableIndexOid, AccessExclusiveLock);
	Relation relTable = table_open(relTableOid, AccessShareLock);

	IndexInfo* relTableIndexInfo = BuildDummyIndexInfo(relTableIndex);
	RelationTruncate(relTableIndex, 0);
	index_build(relTable, relTableIndex, relTableIndexInfo, true, false);
	table_close(relTable, AccessShareLock);
	index_close(relTableIndex, AccessExclusiveLock);
	elog(LOG, "Clearing of Level 0 complete");
}


static bool mylsmInsert(Relation relTableIndex, Datum *values, bool *isnull, ItemPointer ht_ctid, Relation relTable, IndexUniqueCheck checkUnique,bool indexUnchanged,IndexInfo *relTableIndexInfo)
{	
	elog(LOG, "Insertion Start in Mylsm");
	bool output;
    
    //pin and lock BTMETA page.
    pgBtMeta = get_buffer_page(relTableIndex);
    mylsmInfo* pgMyLsmInfo;
	pgMyLsmInfo = (mylsmInfo*)(pgBtMeta+1);

	if (pgMyLsmInfo -> lev0full == false){
		elog(LOG, "Inserting in MyLSM Level 0");
        release_buffer_page(relTableIndex,buf);
        
        //Change the access method for table.
		Oid tempAm;
		tempAm = relTableIndex -> rd_rel -> relam;
		relTableIndex -> rd_rel -> relam = BTREE_AM_OID;
        
        //Insert the tuple using Bteree insert method
		output = btinsert(relTableIndex, values, isnull, ht_ctid, relTable, checkUnique, indexUnchanged ,relTableIndexInfo);
        if(!output)
        {
            elog(LOG, "Inserting in level 0 B-tree Pass");
        }
        else
        {
            elog(LOG, "Inserting in level 0 B-tree Fail");
        }
		relTableIndex -> rd_rel -> relam = tempAm;

        pgBtMeta = get_buffer_page(relTableIndex);
		pgMyLsmInfo = (mylsmInfo*)(pgBtMeta+1);

		pgMyLsmInfo -> num_tup += 1;
		pgMyLsmInfo -> num_tup0 += 1;
		if (pgMyLsmInfo -> num_tup0 == 2)
			pgMyLsmInfo -> lev0full = true;

        release_buffer_page(relTableIndex,buf);

		elog(LOG, "Inserted in Mylsm Level 0");
	}

	else{
		elog(LOG, "MyLsm Level 0 is full");
		elog(LOG, "Inserting in MyLSM Level 1");
		mylsmInfo* pgMyLsmInfoTemp;
		pgMyLsmInfoTemp = (mylsmInfo*)palloc(sizeof(mylsmInfo));
		memcpy(pgMyLsmInfoTemp, pgMyLsmInfo,sizeof(mylsmInfo));
        release_buffer_page(relTableIndex,buf);
        
		if (pgMyLsmInfoTemp -> lev1 == InvalidOid){
			elog(LOG, "Level-1 B-tree in not exist");
			mylsmBuildIndexLev1(relTable, relTableIndex, pgMyLsmInfoTemp);
			elog(LOG, "Level-1 B-tree created");
		}

		Relation lev0;
		lev0 = index_open(pgMyLsmInfoTemp -> lev0, AccessShareLock);
		mylsmMerge(pgMyLsmInfoTemp -> lev0, pgMyLsmInfoTemp -> relTable, pgMyLsmInfoTemp -> lev1);
		mylsmTruncate(pgMyLsmInfoTemp -> lev0, pgMyLsmInfoTemp -> relTable);
		index_close(lev0, AccessShareLock);

        pgBtMeta = get_buffer_page(relTableIndex);
		pgMyLsmInfo = (mylsmInfo*)(pgBtMeta+1);
        pgMyLsmInfo -> num_tup = 0;
        pgMyLsmInfo -> num_tup0 = 0;
        pgMyLsmInfo -> lev0full = false;
        pgMyLsmInfo -> user_info = pgMyLsmInfoTemp -> user_info;
		pgMyLsmInfo -> db_info = pgMyLsmInfoTemp -> db_info;
        pgMyLsmInfo -> relTable = pgMyLsmInfoTemp -> relTable;
		pgMyLsmInfo -> lev1 = pgMyLsmInfoTemp -> lev1;
		pgMyLsmInfo -> lev0 = pgMyLsmInfoTemp -> lev0;
		
        release_buffer_page(relTableIndex,buf);
		pfree(pgMyLsmInfoTemp);
        elog(LOG, "Transfer in Level-1 is done");
        
        Oid tempAm;
        tempAm = relTableIndex -> rd_rel -> relam;
        relTableIndex -> rd_rel -> relam = BTREE_AM_OID;
        output = btinsert(relTableIndex, values, isnull, ht_ctid, relTable, checkUnique, indexUnchanged ,relTableIndexInfo);
        if(!output)
        {
            elog(LOG, "Inserting in level 0 B-tree Pass");
        }
        else
        {
            elog(LOG, "Inserting in level 0 B-tree Fail");
        }
        relTableIndex -> rd_rel -> relam = tempAm;

        pgBtMeta = get_buffer_page(relTableIndex);
        pgMyLsmInfo = (mylsmInfo*)(pgBtMeta+1);

        pgMyLsmInfo -> num_tup += 1;
        pgMyLsmInfo -> num_tup0 += 1;
        if (pgMyLsmInfo -> num_tup0 == 2)
            pgMyLsmInfo -> lev0full = true;

        release_buffer_page(relTableIndex,buf);
	}
	elog(LOG, "Insertion complete");
	return output;
}


Datum mylsm_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine;
	amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 	VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = mylsmBuildIndexLev0;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = mylsmInsert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = btoptions;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = btbeginscan;
	amroutine->amrescan = btrescan;
	amroutine->amgettuple = btgettuple;
	amroutine->amgetbitmap = btgetbitmap;
	amroutine->amendscan = btendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}
