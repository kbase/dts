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
target_compile_definitions(test_subst_env_var PRIVATE TEST)
add_test(test_subst_env_var test_subst_env_var)

add_executable(test_bucket_mapper plugins/bucket_mapper.c)
target_compile_definitions(test_bucket_mapper PRIVATE TEST)
target_link_libraries(test_bucket_mapper PRIVATE ${CMAKE_DL_LIBRARIES})
add_test(test_bucket_mapper test_bucket_mapper "${CMAKE_BINARY_DIR}/libirods_s3_api_bucket_mapper.so")

add_executable(test_user_mapper plugins/user_mapper.c)
target_compile_definitions(test_user_mapper PRIVATE TEST)
target_link_libraries(test_user_mapper PRIVATE ${CMAKE_DL_LIBRARIES})
add_test(test_user_mapper test_user_mapper "${CMAKE_BINARY_DIR}/libirods_s3_api_user_mapper.so")
