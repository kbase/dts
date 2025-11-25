cmake_minimum_required(VERSION 3.22.0)
project (irods_s3_dts_plugins VERSION 3.0.0)

enable_language(C)
set(CMAKE_C_STANDARD 11)

add_library(irods_s3_api_bucket_mapper MODULE
  plugins/bucket_mapper.c
  plugins/subst_env_var.c
  plugins/cJSON.c
)
add_library(irods_s3_api_user_mapper MODULE
  plugins/user_mapper.c
  plugins/subst_env_var.c
  plugins/cJSON.c
)

enable_testing()

add_executable(test_subst_env_var plugins/subst_env_var.c)
target_compile_definitions(test_subst_env_var PUBLIC TEST)
add_test(test_subst_env_var test_subst_env_var)
