#ifndef PLUGIN_TOOLS_H
#define PLUGIN_TOOLS_H

#include "cJSON.h"

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// Parses the data in the file whose path is specified in the 'file_path' field of the JSON object
// defined in the given JSON string, returning a JSON object on success and NULL on failure.
cJSON *read_plugin_config_file(const char *json_string);

// Replaces any environment variable name ($var or ${var}) in s with its value, placing the
// string with substitutions in subst, truncating it at the given maximum length. Returns 0 on
// success, nonzero on failure.
int subst_env_var(const char *s, char *subst, size_t max_len);

#ifdef __cplusplus
} // extern "C"
#endif

#endif
