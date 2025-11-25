#include "user_mapping.h"
#include "subst_env_var.h"
#include "cJSON.h"

#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_STR_LEN      1024
#define MAX_NUM_MAPPINGS 8

typedef struct {
  struct {
    char s3_access_key_id[MAX_STR_LEN];
    char s3_secret_key[MAX_STR_LEN];
    char irods_username[MAX_STR_LEN];
  } mappings[MAX_NUM_MAPPINGS];
  int num_mappings;
} user_mappings_t;

static user_mappings_t global = {0};

int user_mapping_init(const char* _json) {
  cJSON *config = cJSON_Parse(_json);
  if (!config) {
    fprintf(stderr, "ERROR: couldn't parse JSON configuration");
    global = (user_mappings_t){0};
    return 1;
  }

  if (!cJSON_IsObject(config)) {
    fprintf(stderr, "ERROR: JSON configuration is not an object");
    global = (user_mappings_t){0};
    return 1;
  }
  
  global.num_mappings = cJSON_GetArraySize(config);
  if (global.num_mappings > MAX_NUM_MAPPINGS) {
    fprintf(stderr, "ERROR: Number of mappings (%d) exceeds maximum (%d)", global.num_mappings, MAX_NUM_MAPPINGS);
    global = (user_mappings_t){0};
    return 1;
  }

  cJSON *item;
  int i = 0;
  cJSON_ArrayForEach(item, config) {
    if (!cJSON_IsObject(item)) {
      fprintf(stderr, "ERROR: mapping for S3 access key ID '%s' is not an object", item->string);
      global = (user_mappings_t){0};
      return 1;
    }
    if (!cJSON_HasObjectItem(item, "secret_key")) {
      fprintf(stderr, "ERROR: mapping for S3 access key ID '%s' has no 'secret_key' field", item->string);
      global = (user_mappings_t){0};
      return 1;
    }
    if (!cJSON_HasObjectItem(item, "username")) {
      fprintf(stderr, "ERROR: mapping for S3 access key ID '%s' has no 'username' field", item->string);
      global = (user_mappings_t){0};
      return 1;
    }

    cJSON *secret_key = cJSON_GetObjectItemCaseSensitive(item, "secret_key");
    if (!cJSON_IsString(secret_key)) {
      fprintf(stderr, "ERROR: S3 secret key for S3 access key ID '%s' is not a string", item->string);
      global = (user_mappings_t){0};
      return 1;
    }

    cJSON *username   = cJSON_GetObjectItemCaseSensitive(item, "username");
    if (!cJSON_IsString(username)) {
      fprintf(stderr, "ERROR: iRODS username for S3 access key ID '%s' is not a string", item->string);
      global = (user_mappings_t){0};
      return 1;
    }

    // replace any environment variables we find
    subst_env_var(global.mappings[i].s3_access_key_id, item->string, MAX_STR_LEN);
    subst_env_var(global.mappings[i].s3_secret_key, cJSON_GetStringValue(secret_key), MAX_STR_LEN);
    subst_env_var(global.mappings[i].irods_username, cJSON_GetStringValue(username), MAX_STR_LEN);
    ++i;
  }

  cJSON_Delete(config);
  return 0;
}

int user_mapping_irods_username(const char* _s3_access_key_id, char** _irods_username) {
  if (!global.num_mappings) {
    fprintf(stderr, "ERROR: couldn't fetch iRODS username for S3 access key '%s' (invalid mapping state)", _s3_access_key_id);
    *_irods_username = NULL;
    return 1;
  }
  for (int i = 0; i < global.num_mappings; ++i) {
    if (!strncmp(_s3_access_key_id, global.mappings[i].s3_access_key_id, MAX_STR_LEN)) {
      *_irods_username = global.mappings[i].irods_username;
      return 0;
    }
  }
  fprintf(stderr, "ERROR: iRODS username not found for S3 access key '%s'", _s3_access_key_id);
  return 1;
}

int user_mapping_s3_secret_key(const char* _s3_access_key_id, char** _s3_secret_key) {
  if (!global.num_mappings) {
    fprintf(stderr, "ERROR: couldn't fetch S3 secret key for access key '%s' (invalid mapping state)", _s3_access_key_id);
    *_s3_secret_key = NULL;
    return 1;
  }
  for (int i = 0; i < global.num_mappings; ++i) {
    if (!strncmp(_s3_access_key_id, global.mappings[i].s3_access_key_id, MAX_STR_LEN)) {
      *_s3_secret_key = global.mappings[i].s3_secret_key;
      return 0;
    }
  }
  fprintf(stderr, "ERROR: S3 secret key not found for access key '%s'", _s3_access_key_id);
  return 1;
}

int user_mapping_close() {
  global = (user_mappings_t){0};
  return 0;
}

void user_mapping_free(void* _data) {
}

#ifdef __cplusplus
} // extern "C"
#endif
