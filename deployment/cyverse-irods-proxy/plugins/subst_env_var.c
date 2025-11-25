#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_STR_LEN 1024

// replaces any environment variable name ($var or ${var}) in s with its value, placing the
// string with substitutions in subst; returns 0 on success and non-zero on failure
int subst_env_var(char *s, char *subst, size_t max_len) {
  if (max_len > MAX_STR_LEN) {
    fprintf(stderr, "ERROR: environment variable substitution lenth (%zd) exceeds maximum (%d)", max_len, MAX_STR_LEN);
    return 1;
  }

  size_t s_pos = 0, subst_pos = 0;
  while (s_pos < strlen(s) && subst_pos < max_len) {
    char *begin_bracket = strstr(&s[s_pos], "${");
    char *end_bracket = strstr(begin_bracket, "}");
    if (begin_bracket && end_bracket) { // ${var}
      size_t copied_segment_len = begin_bracket - &s[s_pos];
      strlcpy(&subst[subst_pos], &s[s_pos], copied_segment_len);
      s_pos     += copied_segment_len;
      subst_pos += copied_segment_len;

      char env_var_name[MAX_STR_LEN];
      size_t env_var_name_len = end_bracket - begin_bracket - 3; // exclude brackets
      strlcpy(env_var_name, begin_bracket + 2, env_var_name_len);
      char *env_var_value = getenv(env_var_name);
      if (env_var_value) { // substitute env var
        size_t env_var_value_len = strlen(env_var_value);
        strlcpy(&subst[subst_pos], env_var_value, env_var_value_len);
        s_pos     += env_var_name_len + 3; // include brackets
        subst_pos += env_var_value_len;
      } else { // copy env var name
        strlcpy(&subst[subst_pos], env_var_name, env_var_name_len);
        s_pos     += env_var_name_len + 3; // include brackets
        subst_pos += env_var_name_len + 3; // "
      }
    } else { // $var
      char *dollar = strstr(&s[s_pos], "$");
      if (dollar) {
        char env_var_name[MAX_STR_LEN];
        size_t env_var_name_len = 0;
        while (isalnum(dollar[1 + env_var_name_len]) || dollar[1 + env_var_name_len] == '_') {
          ++env_var_name_len;
        }
        if (env_var_name_len > 0) { // substitute env var
          char env_var_name[MAX_STR_LEN];
          size_t env_var_name_len = end_bracket - begin_bracket - 1; // exclude $
          strlcpy(env_var_name, begin_bracket + 1, env_var_name_len);

          char *env_var_value = getenv(env_var_name);
          if (env_var_value) { // substitute env var
            size_t env_var_value_len = strlen(env_var_value);
            strlcpy(&subst[subst_pos], env_var_value, env_var_value_len);
            s_pos     += env_var_name_len + 1; // include $
            subst_pos += env_var_value_len;
          } else { // copy env var name
            strlcpy(&subst[subst_pos], env_var_name, env_var_name_len);
            s_pos     += env_var_name_len + 1; // include $
            subst_pos += env_var_name_len + 1; // "
          }
        } else { // sometimes a $ is just a $
          subst[subst_pos] = '$';
          s_pos     += 1;
          subst_pos += 1;
        }
      }
    }
  }
  return 0;
}

#ifdef __cplusplus
} // extern "C"
#endif
