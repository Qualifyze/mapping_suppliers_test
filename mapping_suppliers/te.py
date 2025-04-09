source_1_length = 2681
print((source_1_length // 10))
for i in range(1, source_1_length + 1):
    if i % (source_1_length // 10) == 0:
        print(f"Processing {i} out of {source_1_length}...")
    # Simulate some processing
    pass