Base public material:
a57 (article 57) : europe manufacturers information
cep : europe suppliers information

ob (orange book) : usa manufacturers information
usdmf : usa suppliers information

Base QF material:
qf_supplier_sites_names : suppliers sites names (sql_qf_supplier_sites_names)
sql_qf_supplier_sites_products : suppliers sites "requested" products (sql_qf_supplier_sites_products)

Raw data : raw_supplier_mapping_test

Steps :

1. Add a clean version of the ids to the base file (clean_sources.py)

2. Generate list of public supplier names : cep + usdmf (full_names.py)
3. Generate list of public manufacturer required api : a57 + ob (full_manufacturer_required.py)

4. Create the mappings
    a. Mapping list
        - a57 <-> cep : substance_a57_to_cep
        - OB <-> USDMF : substance_orange_book_to_usdmf
        - Public Supplier <-> QF : supplier_public_to_qf
        - Public Manufacturer required API <-> QF : substance_public_manufacturer_to_qf

    For each mapping
    b. Generate the batches
        The following command will generate the batches for the mapping
        python main.py {{ mapping_name }} -g (-d to dry run)
    c. Process the mapping
        The following command will process the batches for the mapping
        python main.py {{ mapping_name }} -p

5. Run the sql to create the final table
    a. sql_europe_union_usa.sql
    b. sql_final.sql
