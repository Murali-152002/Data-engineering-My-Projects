select * from read_parquet('{{ env_var("CL_LAKE") }}/silver_export/partd/*.parquet')
