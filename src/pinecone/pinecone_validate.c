#include "pinecone.h"

#include <access/reloptions.h>


void validate_api_key(void) {
    if (pinecone_api_key == NULL || strlen(pinecone_api_key) == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Pinecone API key not set"),
                 errhint("Set the pinecone API key using the pinecone.api_key GUC. E.g. ALTER SYSTEM SET pinecone.api_key TO 'your-api-key'")));
    }
}

bool validate_vector_nonzero(Vector* vector) {
    if (vector_eq_zero_internal(vector)) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector: zero vector"),
                        errhint("Pinecone insists that dense vectors cannot be zero in all dimensions. I don't know why they do this to you even when your metric isn't cosine.")));
        return false;
    }
    return true;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnonnull"
void pinecone_spec_validator(const PineconeOptions *opts)
{
    if (opts == NULL || cJSON_Parse(GET_STRING_RELOPTION(opts, spec)) == NULL || strcmp(GET_STRING_RELOPTION(opts, spec), "") == 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid spec"),
                 errhint("Spec should be a valid JSON object e.g. WITH (spec='{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}').\n \
                         Refer to https://docs.pinecone.io/reference/create_index")));
    }
}
#pragma GCC diagnostic pop

void pinecone_host_validator(const char *host)
{
    return;
}


bool no_validate(Oid opclassoid) { return true; }

