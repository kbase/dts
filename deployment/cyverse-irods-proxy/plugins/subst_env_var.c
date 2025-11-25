#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_STR_LEN 1024

// Replaces any environment variable name (${var}) in s with its value, placing the
// string with substitutions in subst. No nested environment variables are allowed.
// Returns 0 on success and non-zero on failure.
int subst_env_var(const char *s, char *subst, size_t max_len) {
  if (max_len > MAX_STR_LEN) {
    fprintf(stderr, "ERROR: environment variable substitution length (%zd) exceeds maximum (%d)", max_len, MAX_STR_LEN);
    return 1;
  }
  size_t s_len = strlen(s);
  if (s_len > MAX_STR_LEN) {
    fprintf(stderr, "ERROR: string substitution length (%zd) exceeds maximum (%d)", s_len, MAX_STR_LEN);
    return 1;
  }

  size_t s_pos = 0, subst_pos = 0;
  while (s_pos < s_len && subst_pos < max_len) {
    char *begin_bracket = strstr(&s[s_pos], "${");
    if (begin_bracket) {
      char *end_bracket = strstr(begin_bracket, "}");
      if (end_bracket) { // ${var}
        size_t copied_segment_len = begin_bracket - &s[s_pos];
        memcpy(&subst[subst_pos], &s[s_pos], copied_segment_len);
        s_pos     += copied_segment_len;
        subst_pos += copied_segment_len;

        char env_var_name[MAX_STR_LEN];
        size_t env_var_name_len = end_bracket - begin_bracket - 1; // exclude brackets
        strlcpy(env_var_name, begin_bracket + 2, env_var_name_len);
        char *env_var_value = getenv(env_var_name);
        if (env_var_value) { // substitute env var
          size_t env_var_value_len = strlen(env_var_value);
          memcpy(&subst[subst_pos], env_var_value, env_var_value_len);
          s_pos     += env_var_name_len + 2; // include brackets
          subst_pos += env_var_value_len;
        } else { // copy env var name
          memcpy(&subst[subst_pos], env_var_name, env_var_name_len);
          s_pos     += env_var_name_len + 2; // include brackets
          subst_pos += env_var_name_len + 2; // "
        }
      } else { // unclosed bracket -- no substitution
        memcpy(&subst[subst_pos], &s[s_pos], s_len - s_pos);
        subst_pos += s_len - s_pos;
        s_pos      = s_len;
      }
    } else { // no substitution
      memcpy(&subst[subst_pos], &s[s_pos], s_len - s_pos);
      subst_pos += s_len - s_pos;
      s_pos      = s_len;
    }
  }
  return 0;
}

#ifdef TEST

int main(int argc, char *argv[]) {
  static const char *s =
    "{\n"
    "  \"${S3_ACCESS_KEY_ID}\": {\n"
    "    \"secret_key\": \"${S3_SECRET_KEY}\",\n"
    "    \"username\": \"${IRODS_USERNAME}\",\n"
    "  }\n"
    "}";

  static const char env_var_names[3][MAX_STR_LEN] = {
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_KEY",
    "IRODS_USERNAME",
  };
  const char env_var_values[3][MAX_STR_LEN] = {
    "s3-user-1234567",
    "s3-sekret-1234567",
    "irods-user",
  };
  for (int i = 0; i < 3; ++i) {
    setenv(env_var_names[i], env_var_values[i], 1);
  }

  char subst[MAX_STR_LEN] = {0};
  int result = subst_env_var(s, subst, MAX_STR_LEN);
  if (result) {
    exit(result);
  }

  static const char *ref_subst =
    "{\n"
    "  \"s3-user-1234567\": {\n"
    "    \"secret_key\": \"s3-sekret-1234567\",\n"
    "    \"username\": \"irods-user\",\n"
    "  }\n"
    "}";
  result = strncmp(subst, ref_subst, MAX_STR_LEN);
  if (result) {
    fprintf(stderr, "ERROR: mismatch in substituted string:\nSubstitution: %s\nShould be: %s\n",
            subst, ref_subst);
    exit(result);
  }

  return 0;
}

#endif

#ifdef __cplusplus
} // extern "C"
#endif
