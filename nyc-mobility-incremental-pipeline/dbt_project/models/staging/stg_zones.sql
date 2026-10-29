select * from read_parquet('{{ env_var("NYC_EXPORT") }}/zones/*.parquet')
