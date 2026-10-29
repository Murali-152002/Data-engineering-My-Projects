select * from read_parquet('{{ env_var("NYC_EXPORT") }}/trips/*/*.parquet', hive_partitioning = true)
