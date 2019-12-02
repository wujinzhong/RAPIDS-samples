import cudf

def generate_file(output_file,rows=100):
    with open(output_file, 'wb') as f:
        f.write(b'A,B,C,D,E,F,G,H,I,J,K\n')
        f.write(b'22,697,56,0.0,0.0,0.0,0.0,0.0,0.0,0,0\n23,697,56,0.0,0.0,0.0,0.0,0.0,0.0,0,0\n'*(rows//2))
        f.close()

# generate the test file
csv_file='test_1_000_000_000.csv'
#generate_file(csv_file,rows=1_000_000_000)

def initialize_rmm():
    import rmm
    from rmm import rmm_config as rmm_cfg
    import cudf
    rmm.finalize()
    rmm_cfg.use_managed_memory = True
    rmm.initialize()
    print(rmm_cfg.use_managed_memory)

initialize_rmm()
df = cudf.read_csv(csv_file,chunksize='100 KiB')
print(df.head(10).to_pandas())
df2 = df.sort_values(['A','B','C'])
