    

  create  table
    "qualifyze_dwh"."raw_supplier_mapping_test"."europe_union_usa"
    
    
    
  as (
with

europe as (
    select
        'eu' as source,
        a57.marketing_auth_holder as manufacturer_name,
        a57.product_name as drug_parent_name,
        a57.route_of_admin as administration_route,
        a57.active_substance as manufacturer_required_api,
        a57.active_substance_cleaned as manufacturer_required_api_cleaned,
        null as manufacturer_required_api_strength,
        a57.is_discontinued = 'True' as is_discontinued,
        cep.certificateHolder as supplier_name,
        cep.certificateholder_cleaned as supplier_name_cleaned,
        cep.englishName as supplier_offered_api,
        substance_a57_to_cep_mapping.have_same_base = 'True' as supplier_offered_api_has_same_base,
        substance_a57_to_cep_mapping.have_same_form = 'True' as supplier_offered_api_has_same_form,
        substance_a57_to_cep_mapping.is_diluted = 'True' as supplier_offered_api_is_diluted
    from raw_supplier_mapping_test.a57
    left join raw_supplier_mapping_test.substance_a57_to_cep_mapping
        on a57.active_substance_cleaned = substance_a57_to_cep_mapping.a57_mapped_substance_cleaned
    left join raw_supplier_mapping_test.cep
        on substance_a57_to_cep_mapping.cep_mapped_substance_cleaned = cep.englishname_cleaned
),

usa as (
    select
        'usa' as source,
        ob.applicant_full_name as manufacturer_name,
        ob.trade_name as drug_parent_name,
        ob."df;route" as administration_route,
        ob.ingredient as manufacturer_required_api,
        ob.ingredient_cleaned as manufacturer_required_api_cleaned,
        ob.strength as manufacturer_required_api_strength,
        ob.is_discontinued = 'True' as is_discontinued,
        usdmf.holder as supplier_name,
        usdmf.holder_cleaned as supplier_name_cleaned,
        usdmf.subject as supplier_offered_api,
        substance_orange_book_to_usdmf_mapping.have_same_base = 'True' as supplier_offered_api_has_same_base,
        substance_orange_book_to_usdmf_mapping.have_same_form = 'True' as supplier_offered_api_has_same_form,
        substance_orange_book_to_usdmf_mapping.is_diluted = 'True' as supplier_offered_api_is_diluted
    from raw_supplier_mapping_test.ob
    left join raw_supplier_mapping_test.substance_orange_book_to_usdmf_mapping
        on ob.ingredient_cleaned = substance_orange_book_to_usdmf_mapping.ob_mapped_substance_cleaned
    left join raw_supplier_mapping_test.usdmf
        on substance_orange_book_to_usdmf_mapping.us_dmf_mapped_substance_cleaned = usdmf.subject_cleaned
),

europe_union_usa as (
    select * from europe
    union all
    select * from usa
)

select distinct * from europe_union_usa
  );