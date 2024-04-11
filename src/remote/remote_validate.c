#include "remote.h"

#include <access/reloptions.h>


void validate_vector_nonzero(Vector* vector) {
    if (vector_eq_zero_internal(vector)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector: zero vector"),
                        errhint("Remote insists that dense vectors cannot be zero in all dimensions. I don't know why they do this to you even when your metric isn't cosine.")));
    }
}


void remote_spec_validator(const char *spec)
{
    bool empty = strcmp(spec, "") == 0;
    if (empty || cJSON_Parse(spec) == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (empty ? errmsg("Spec cannot be empty") : errmsg("Invalid spec: %s", spec)),
                errhint("Spec should be a valid JSON object e.g. WITH (spec='{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}').\n \
                        Refer to https://docs.remote.io/reference/create_index")));
    }
}

void remote_host_validator(const char *host)
{
    return;
}


bool no_validate(Oid opclassoid) { return true; }

