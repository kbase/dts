#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// replaces any environment variable name ($var or ${var}) in s with its value, placing the
// string with substitutions in subst, truncating it at the given maximum length
void subst_env_var(char *s, char *subst, size_t max_len);

#ifdef __cplusplus
} // extern "C"
#endif
