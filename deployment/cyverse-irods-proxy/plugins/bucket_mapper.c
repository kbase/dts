#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_STR_LEN      1024
#define MAX_NUM_MAPPINGS 8

#ifndef TEST

#include "bucket_mapping.h"
#include "plugin_tools.h"

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
  cJSON *plugin_config = read_plugin_config_file(_json);
  if (!plugin_config) {
    global = (bucket_mappings_t){0};
    return 1;
  }

  global.num_mappings = cJSON_GetArraySize(plugin_config);
  if (global.num_mappings > MAX_NUM_MAPPINGS) {
    fprintf(stderr, "ERROR: Number of mappings (%d) exceeds maximum (%d)\n", global.num_mappings, MAX_NUM_MAPPINGS);
    global = (bucket_mappings_t){0};
    return 1;
  }

  cJSON *item;
  int i = 0;
  cJSON_ArrayForEach(item, plugin_config) {
    if (!cJSON_IsString(item)) {
      fprintf(stderr, "ERROR: mapping for bucket '%s' is not a string (collection)\n", item->string);
      global = (bucket_mappings_t){0};
      return 1;
    }

    // replace any environment variables we find
    subst_env_var(item->string, global.mappings[i].bucket, MAX_STR_LEN);
    subst_env_var(cJSON_GetStringValue(item), global.mappings[i].collection, MAX_STR_LEN);
    ++i;
  }

  cJSON_Delete(plugin_config);
  return 0;
}

int bucket_mapping_list(bucket_mapping_entry_t** _buckets, size_t* _size) {
  if (!global.num_mappings) {
    fprintf(stderr, "ERROR: couldn't fetch bucket mapping list (invalid mapping state)\n");
    *_buckets = NULL;
    *_size = 0;
    return 1;
  }
  *_size = global.num_mappings;
  *_buckets = calloc(*_size, sizeof(bucket_mapping_entry_t)); 
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
    fprintf(stderr, "ERROR: couldn't fetch collection for bucket '%s' (invalid mapping state\n)", _bucket);
    *_collection = NULL;
    return 1;
  }
  for (int i = 0; i < global.num_mappings; ++i) {
    if (!strncmp(_bucket, global.mappings[i].bucket, MAX_STR_LEN)) {
      *_collection = global.mappings[i].collection;
      return 0;
    }
  }
  fprintf(stderr, "ERROR: collection not found for bucket '%s'\n", _bucket);
  return 1;
}

int bucket_mapping_close() {
  global = (bucket_mappings_t){0};
  return 0;
}

void bucket_mapping_free(void* _data) {
  free(_data);
}

#endif

#ifdef TEST

#include <dlfcn.h>
#include <stdlib.h>

typedef struct bucket_mapping_entry // copied from bucket_mapping.h
{
	char* bucket;
	char* collection;
} bucket_mapping_entry_t;

static int (*bucket_mapping_init)(const char *) = NULL;
static int (*bucket_mapping_list)(bucket_mapping_entry_t **, size_t *) = NULL;
static int (*bucket_mapping_collection)(const char *, char **) = NULL;
static int (*bucket_mapping_close)(void) = NULL;
static void (*bucket_mapping_free)(void *) = NULL;

int main(int argc, char *argv[]) {

  if (argc == 1) {
    fprintf(stderr, "ERROR: execute with path to plugin\n");
    exit(1);
  }

  // see ../bucket-mapping.json for details
  static const char *config_s = 
    "{\n"
    "  \"file_path\": \"../bucket-mapping.json\"\n"
    "}";

  setenv("S3_BUCKET_NAME", "iplant", 1);
  setenv("IRODS_COLLECTION", "collection_1", 1);

  void *plugin = dlopen(argv[1], RTLD_NOW);
  if (!plugin) {
    fprintf(stderr, "ERROR: couldn't load plugin\n");
    exit(1);
  }

  bucket_mapping_init = dlsym(plugin, "bucket_mapping_init");
  bucket_mapping_list = dlsym(plugin, "bucket_mapping_list");
  bucket_mapping_collection = dlsym(plugin, "bucket_mapping_collection");
  bucket_mapping_close = dlsym(plugin, "bucket_mapping_close");
  bucket_mapping_free = dlsym(plugin, "bucket_mapping_free");

  if (!bucket_mapping_init) { fprintf(stderr, "ERROR: couldn't load bucket_mapping_init\n"); exit(1); }
  if (!bucket_mapping_list) { fprintf(stderr, "ERROR: couldn't load bucket_mapping_list\n"); exit(1); }
  if (!bucket_mapping_collection) { fprintf(stderr, "ERROR: couldn't load bucket_mapping_collection\n"); exit(1); }
  if (!bucket_mapping_close) { fprintf(stderr, "ERROR: couldn't load bucket_mapping_close\n"); exit(1); }
  if (!bucket_mapping_free) { fprintf(stderr, "ERROR: couldn't load bucket_mapping_free\n"); exit(1); }

  int result = bucket_mapping_init(config_s);
  if (result) {
    exit(result);
  }

  bucket_mapping_entry_t *buckets;
  size_t size;

  result = bucket_mapping_list(&buckets, &size);
  if (result) {
    exit(result);
  }
  if (!buckets) {
    fprintf(stderr, "ERROR: no buckets!\n");
    exit(1);
  }
  if (size != 1) {
    fprintf(stderr, "ERROR: wrong number of buckets (%zd, should be 1)\n", size);
    exit(1);
  }
  bucket_mapping_free(buckets);

  char *collection;
  result = bucket_mapping_collection("iplant", &collection);
  if (result) {
    exit(result);
  }
  if (!collection) {
    fprintf(stderr, "ERROR: no collection for bucket 'iplant'!\n");
    exit(1);
  }
  if (strncmp(collection, "collection_1", MAX_STR_LEN)) {
    fprintf(stderr, "ERROR: wrong collection for bucket 'iplant' ('%s', should be 'collection_1'\n)",
            collection);
    exit(1);
  }

  result = bucket_mapping_close();
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
