#include "bucket_mapping.h"
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
    char bucket[MAX_STR_LEN];
    char collection[MAX_STR_LEN];
  } mappings[MAX_NUM_MAPPINGS];
  int num_mappings;

  bucket_mapping_entry_t buckets[MAX_NUM_MAPPINGS];
} bucket_mappings_t;

static bucket_mappings_t global = {0};

int bucket_mapping_init(const char* _json) {
  cJSON *config = cJSON_Parse(_json);
  if (!config) {
    fprintf(stderr, "ERROR: couldn't parse JSON configuration");
    global = (bucket_mappings_t){0};
    return 1;
  }

  if (!cJSON_IsObject(config)) {
    fprintf(stderr, "ERROR: JSON configuration is not an object");
    global = (bucket_mappings_t){0};
    return 1;
  }
  
  global.num_mappings = cJSON_GetArraySize(config);
  if (global.num_mappings > MAX_NUM_MAPPINGS) {
    fprintf(stderr, "ERROR: Number of mappings (%d) exceeds maximum (%d)", global.num_mappings, MAX_NUM_MAPPINGS);
    global = (bucket_mappings_t){0};
    return 1;
  }

  cJSON *item;
  int i = 0;
  cJSON_ArrayForEach(item, config) {
    if (!cJSON_IsString(item)) {
      fprintf(stderr, "ERROR: mapping for bucket '%s' is not a string (collection)", item->string);
      global = (bucket_mappings_t){0};
      return 1;
    }

    // replace any environment variables we find
    subst_env_var(global.mappings[i].bucket, item->string, MAX_STR_LEN);
    subst_env_var(global.mappings[i].collection, cJSON_GetStringValue(item), MAX_STR_LEN);
    ++i;
  }

  cJSON_Delete(config);
  return 0;
}

int bucket_mapping_list(bucket_mapping_entry_t** _buckets, size_t* _size) {
  if (!global.num_mappings) {
    fprintf(stderr, "ERROR: couldn't fetch bucket mapping list (invalid mapping state)");
    *_buckets = NULL;
    *_size = 0;
    return 1;
  }
  *_size = global.num_mappings;
  for (int i = 0; i < global.num_mappings; ++i) {
    (*_buckets)[i] = (bucket_mapping_entry_t){
      .bucket = global.mappings[i].bucket,
      .collection = global.mappings[i].collection,
    };
  }
  return 0;
}

int bucket_mapping_collection(const char* _bucket, char** _collection) {
  if (!global.num_mappings) {
    fprintf(stderr, "ERROR: couldn't fetch collection for bucket '%s' (invalid mapping state)", _bucket);
    *_collection = NULL;
    return 1;
  }
  for (int i = 0; i < global.num_mappings; ++i) {
    if (!strncmp(_bucket, global.mappings[i].bucket, MAX_STR_LEN)) {
      *_collection = global.mappings[i].collection;
      return 0;
    }
  }
  fprintf(stderr, "ERROR: collection not found for bucket '%s'", _bucket);
  return 1;
}

int bucket_mapping_close() {
  global = (bucket_mappings_t){0};
  return 0;
}

void bucket_mapping_free(void* _data) {
}

#ifdef __cplusplus
} // extern "C"
#endif
