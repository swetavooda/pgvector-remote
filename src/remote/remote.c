

#include <float.h>  // DBL_MAX

#include "remote/remote.h"

#include "utils/guc.h" // DefineCustomIntVariable
#include "utils/rel.h" // Relation
#include "utils/selfuncs.h" // cost estimate utilities

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

int remote_top_k = 1000; // todo: dynamic meta w/ helper fn
int remote_requests_per_batch = 10;  // todo: static meta
int remote_max_buffer_scan = 10000; // maximum number of tuples to search in the buffer // todo: dynamic meta w/ helper fn
int remote_max_fetched_vectors_for_liveness_check = 10;  // todo: dynamic meta w/ helper fn
#ifdef REMOTE_MOCK
bool remote_use_mock_response = false;
#endif

static relopt_kind remote_relopt_kind;


void RemoteInit(void)
{
    initialize_remote_index_interfaces();

    remote_relopt_kind = add_reloption_kind();
    // N.B. The default values are validated when the extension is created, so we have to provide a valid json default
    add_string_reloption(remote_relopt_kind, "spec",
                            "Specification of the Remote Index. Refer to https://docs.remote.io/reference/create_index",
                            DEFAULT_SPEC,
                            NULL,
                            AccessExclusiveLock);
    add_enum_reloption(remote_relopt_kind, "provider",
                            "Remote provider. Currently pinecone and milvus are supported",
                            provider_enum_options,
                            PINECONE_PROVIDER,
                            "detail msg",
                            AccessExclusiveLock);
    add_string_reloption(remote_relopt_kind, "host",
                            "Host of the Remote Index. Cannot be used with spec",
                            DEFAULT_HOST,
                            NULL,
                            AccessExclusiveLock);
    add_bool_reloption(remote_relopt_kind, "overwrite",
                            "Delete all vectors in existing index. Host must be specified",
                            false, AccessExclusiveLock);
    add_bool_reloption(remote_relopt_kind, "skip_build",
                            "Do not upload vectors from the base table.",
                            false, AccessExclusiveLock);
    add_int_reloption(remote_relopt_kind, "batch_size",
                            "Number of vectors in each buffer batch. (N.B. pinecone will split this into smaller concurrent requests based on remote.pinecone_vectors_per_request)",
                            1000, 1, 10000, AccessExclusiveLock);
    DefineCustomIntVariable("remote.top_k", "Remote top k", "Remote top k",
                            &remote_top_k,
                            500, 1, 10000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("remote.requests_per_batch", "Remote requests per batch", "Remote requests per batch",
                            &remote_requests_per_batch,
                            10, 1, 100,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("remote.max_buffer_scan", "Remote max buffer search", "Remote max buffer search",
                            &remote_max_buffer_scan,
                            10000, 0, 100000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("remote.max_fetched_vectors_for_liveness_check", "Remote max fetched vectors for liveness check", "Remote max fetched vectors for liveness check",
                            &remote_max_fetched_vectors_for_liveness_check,
                            10, 0, 100, // more than 100 is useless and won't fit in the 2048 chars allotted for the URL
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    #ifdef REMOTE_MOCK
    DefineCustomBoolVariable("remote.use_mock_response", "Remote use mock response", "Remote use mock response",
                            &remote_use_mock_response,
                            false,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    #endif
    MarkGUCPrefixReserved("remote");
}

void remote_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
    // adapted from hnsw_costestimate
	GenericCosts costs;
    // open the index to read the provider // TODO: is this expensive?
    Oid indexoid = path->indexinfo->indexoid;
    Relation index = index_open(indexoid, AccessShareLock);
    RemoteOptions* opts = (RemoteOptions *) index->rd_options;
    RemoteIndexInterface* remote_index_interface = remote_index_interfaces[opts->provider];
    index_close(index, AccessShareLock);

	/* Never use index without order */
	if (path->indexorderbys == NULL)
	{
		*indexStartupCost = DBL_MAX;
		*indexTotalCost = DBL_MAX;
		*indexSelectivity = 0;
		*indexCorrelation = 0;
		*indexPages = 0;
		return;
	}

	MemSet(&costs, 0, sizeof(costs));

	/* TODO Improve estimate of visited tuples (currently underestimates) */
	/* Account for number of tuples (or entry level), m, and ef_search */
	costs.numIndexTuples = remote_index_interface->est_network_cost();

	genericcostestimate(root, path, loop_count, &costs);

	/* Use total cost since most work happens before first tuple is returned */
	*indexStartupCost = costs.indexTotalCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
};

bytea * remote_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"spec", RELOPT_TYPE_STRING, offsetof(RemoteOptions, spec)},
		{"provider", RELOPT_TYPE_ENUM, offsetof(RemoteOptions, provider)},
        {"host", RELOPT_TYPE_STRING, offsetof(RemoteOptions, host)},
        {"overwrite", RELOPT_TYPE_BOOL, offsetof(RemoteOptions, overwrite)},
        {"skip_build", RELOPT_TYPE_BOOL, offsetof(RemoteOptions, skip_build)},
        {"batch_size", RELOPT_TYPE_INT, offsetof(RemoteOptions, batch_size)}

	};
    static bool first_time = true;
    bool spec_set, host_set, exactly_one;
    RemoteOptions* opts = (RemoteOptions *) build_reloptions(reloptions, validate,
                                      remote_relopt_kind,
                                      sizeof(RemoteOptions),
                                      tab, lengthof(tab));
    // if this is the first call, we don't want to validate the default values
    if (first_time) {
        first_time = false;
        return (bytea *) opts;
        // this is ugly but otherwise pg tries to validate the default values
    }

    // check that exactly one of spec or host is set
    spec_set = (opts->spec != 0) && (strcmp((char*) opts + opts->spec, DEFAULT_SPEC) != 0);
    host_set = (opts->host != 0) && (strcmp((char*) opts + opts->host, DEFAULT_HOST) != 0);
    exactly_one = spec_set ^ host_set;
    if (!exactly_one) {
        ereport(NOTICE,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Exactly one of spec or host must be set, but host is %s and spec is %s", GET_STRING_RELOPTION(opts, host), GET_STRING_RELOPTION(opts, spec))));
    }
	return (bytea *) opts;
}

bool no_validate(Oid opclassoid) { return true; }

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(remotehandler);
Datum remotehandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = 2; /* number of support functions */
#if PG_VERSION_NUM >= 130000
    amroutine->amoptsprocnum = 0;
#endif
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false; // we only store ids, we don't have a covering index
#if PG_VERSION_NUM >= 130000
    amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
    amroutine->amparallelvacuumoptions = 0;
#endif
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    amroutine->ambuild = remote_build;
    amroutine->ambuildempty = remote_buildempty;
    amroutine->aminsert = remote_insert;
    amroutine->ambulkdelete = remote_bulkdelete;
    amroutine->amvacuumcleanup = no_vacuumcleanup;
    // used to indicate if we support index-only scans; takes a attno and returns a bool;
    // included cols should always return true since there is little point in an included column if it can't be returned
    amroutine->amcanreturn = NULL; // do we support index-only scans?
    amroutine->amcostestimate = remote_costestimate;
    amroutine->amoptions = remote_options;
    amroutine->amproperty = NULL;            /* TODO AMPROP_DISTANCE_ORDERABLE */
    amroutine->ambuildphasename = NULL;      // maps build phase number to name
    amroutine->amvalidate = no_validate; // check that the operator class is valid (provide the opclass's object id)
#if PG_VERSION_NUM >= 140000
    amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = remote_beginscan;
    amroutine->amrescan = remote_rescan;
    amroutine->amgettuple = remote_gettuple;
    amroutine->amgetbitmap = NULL; // an alternative to amgettuple that returns a bitmap of matching tuples
    amroutine->amendscan = no_endscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* Interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
