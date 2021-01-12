add_executable (quantile-t-digest quantile-t-digest.cpp)
target_link_libraries (quantile-t-digest PRIVATE dbms clickhouse_aggregate_functions)

add_executable (uniq-hll64-test uniq_hll64_test.cpp)
target_link_libraries (uniq-hll64-test PRIVATE dbms clickhouse_aggregate_functions)
