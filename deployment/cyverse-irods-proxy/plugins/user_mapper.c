#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_STR_LEN      1024

#ifndef TEST

#include "user_mapping.h"
#include "subst_env_var.h"
#include "cJSON.h"

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

#endif

#ifdef TEST

#include <dlfcn.h>
#include <stdlib.h>

static int (*user_mapping_init)(const char *) = NULL;
static int (*user_mapping_irods_username)(const char *, char **) = NULL;
static int (*user_mapping_s3_secret_key)(const char *, char **) = NULL;
static int (*user_mapping_close)(void) = NULL;
static void (*user_mapping_free)(void *) = NULL;

int main(int argc, char *argv[]) {

  if (argc == 1) {
    fprintf(stderr, "ERROR: execute with path to plugin\n");
    exit(1);
  }

  static const char *config_s =
    "{\n"
    "  \"${user_NAME}\": \"${COLLECTION_NAME}\",\n"
    "}";

  setenv("IRODS_USERNAME", "irods_user", 1);
  setenv("S3_ACCESS_KEY_ID", "s3-user-1234567", 1);
  setenv("S3_SECRET_KEY", "s3-sekret-1234567", 1);

  void *plugin = dlopen(argv[1], RTLD_NOW);
  if (!plugin) {
    fprintf(stderr, "ERROR: couldn't load plugin\n");
    exit(1);
  }

  user_mapping_init = dlsym(plugin, "user_mapping_init");
  user_mapping_irods_username = dlsym(plugin, "user_mapping_irods_username");
  user_mapping_s3_secret_key = dlsym(plugin, "user_mapping_s3_secret_key\n");
  user_mapping_close = dlsym(plugin, "user_mapping_close");
  user_mapping_free = dlsym(plugin, "user_mapping_free");

  if (!user_mapping_init) { fprintf(stderr, "ERROR: couldn't load user_mapping_init\n"); exit(1); }
  if (!user_mapping_irods_username) { fprintf(stderr, "ERROR: couldn't load user_mapping_irods_username\n"); exit(1); }
  if (!user_mapping_s3_secret_key) { fprintf(stderr, "ERROR: couldn't load user_mapping_s3_secret_key\n"); exit(1); }
  if (!user_mapping_close) { fprintf(stderr, "ERROR: couldn't load user_mapping_close\n"); exit(1); }
  if (!user_mapping_free) { fprintf(stderr, "ERROR: couldn't load user_mapping_free\n"); exit(1); }

  int result = user_mapping_init(config_s);
  if (result) {
    exit(result);
  }

  char *irods_username;
  result = user_mapping_irods_username("s3-user-1234567", &irods_username);
  if (result) {
    exit(result);
  }
  if (!irods_username) {
    fprintf(stderr, "ERROR: no iRODS username for s3 access key 's3-user-1234567'!\n");
    exit(1);
  }
  if (strncmp(irods_username, "irods_user", MAX_STR_LEN)) {
    fprintf(stderr, "ERROR: wrong iRODS username for s3 access key 's3-user-1234567' ('%s', should be 'irods_user'\n)",
            irods_username);
    exit(1);
  }

  char *s3_sekret;
  result = user_mapping_irods_username("s3-user-1234567", &s3_sekret);
  if (result) {
    exit(result);
  }
  if (!s3_sekret) {
    fprintf(stderr, "ERROR: no s3 secret for s3 access key 's3-user-1234567'!\n");
    exit(1);
  }
  if (strncmp(irods_username, "s3-sekret-1234567", MAX_STR_LEN)) {
    fprintf(stderr, "ERROR: wrong s3 secret for s3 access key 's3-user-1234567' ('%s', should be 's3-sekret-1234567'\n)",
            irods_username);
    exit(1);
  }

  result = user_mapping_close();
  if (result) {
    exit(result);
  }

  dlclose(plugin);

  return 0;
}

#endif

#ifdef __cplusplus
} // extern "C"
#endif
