    

  create  table
    "qualifyze_dwh"."warehouse"."public_supplier_mapping_to_qf"



  as (
select distinct
    europe_union_usa.source,
    europe_union_usa.manufacturer_name,
    europe_union_usa.drug_parent_name,
    europe_union_usa.administration_route,
    europe_union_usa.manufacturer_required_api,
    europe_union_usa.manufacturer_required_api_strength,
    europe_union_usa.is_discontinued,
    europe_union_usa.supplier_name,
    europe_union_usa.supplier_offered_api,
    europe_union_usa.supplier_offered_api_has_same_base,
    europe_union_usa.supplier_offered_api_has_same_form,
    europe_union_usa.supplier_offered_api_is_diluted,
    qf_supplier_sites_full.qf_supplier_id,
    qf_supplier_sites_full.qf_supplier_site_id,
    qf_supplier_sites_full.qf_supplier_name,
    qf_supplier_sites_full.qf_supplier_site_name,
    qf_supplier_sites_full.qf_supplier_site_audited_quality_types,
    qf_supplier_sites_full.qf_supplier_site_valid_audit_count,
    qf_supplier_sites_full.qf_supplier_site_last_expiration,
    qf_supplier_sites_full.qf_supplier_site_audited_requested_api,
    latest_qcr.qcr as qf_qcr,
    latest_qcr.qcr_date as qf_qcr_date,
    bool_or(qf_supplier_sites_full.qf_is_audit_valid = 'True') as qf_has_valid_audit,
    bool_or(substance_public_manufacturer_to_qf_mapping.have_same_base = 'True') as qf_has_same_base,
    bool_or(substance_public_manufacturer_to_qf_mapping.have_same_form = 'True') as qf_has_same_form,
    bool_and(substance_public_manufacturer_to_qf_mapping.is_diluted = 'True') as qf_is_diluted,
    listagg(
        distinct coalesce(
            case
                when
                    substance_public_manufacturer_to_qf_mapping.have_same_base = 'True'
                    or substance_public_manufacturer_to_qf_mapping.have_same_form = 'True'
                    then qf_supplier_sites_full.qf_supplier_site_audited_requested_product
            end
            , ''
        ),
    ' ||| ') as qf_supplier_site_audited_requested_product,
    bool_or(substance_public_manufacturer_to_qf_mapping.have_same_form = 'True' and substance_public_manufacturer_to_qf_mapping.have_same_form = 'True') as qf_is_exact_match
from raw_supplier_mapping_test.europe_union_usa
left join raw_supplier_mapping_test.supplier_public_to_qf_mapping
    on europe_union_usa.supplier_name_cleaned = supplier_public_to_qf_mapping.public_mapped_supplier_cleaned
left join raw_supplier_mapping_test.qf_supplier_sites as qf_supplier_sites_names
    on supplier_public_to_qf_mapping.qf_mapped_supplier_cleaned = qf_supplier_sites_names.ceapp_supplier_site_name_cleaned
left join raw_supplier_mapping_test.qf_supplier_sites_full on
    qf_supplier_sites_names.ceapp_id_supplier = qf_supplier_sites_full.qf_supplier_id
left join raw_supplier_mapping_test.substance_public_manufacturer_to_qf_mapping on
    europe_union_usa.manufacturer_required_api_cleaned = substance_public_manufacturer_to_qf_mapping.public_mapped_substance_cleaned
    and qf_supplier_sites_full.qf_supplier_site_audited_requested_product_cleaned = substance_public_manufacturer_to_qf_mapping.qf_mapped_substance_cleaned
    and (
         substance_public_manufacturer_to_qf_mapping.have_same_base = 'True'
         or substance_public_manufacturer_to_qf_mapping.have_same_form = 'True'
    )
left join raw_supplier_mapping_test.latest_qcr
    on replace(qf_supplier_sites_full.ceapp_id_supplier_site, '.0', '') = replace(latest_qcr.ceapp_id_supplier_site, '.0', '')
group by
    europe_union_usa.source,
    europe_union_usa.manufacturer_name,
    europe_union_usa.drug_parent_name,
    europe_union_usa.administration_route,
    europe_union_usa.manufacturer_required_api,
    europe_union_usa.manufacturer_required_api_strength,
    europe_union_usa.is_discontinued,
    europe_union_usa.supplier_name,
    europe_union_usa.supplier_offered_api,
    europe_union_usa.supplier_offered_api_has_same_base,
    europe_union_usa.supplier_offered_api_has_same_form,
    europe_union_usa.supplier_offered_api_is_diluted,
    qf_supplier_sites_full.qf_supplier_id,
    qf_supplier_sites_full.qf_supplier_site_id,
    qf_supplier_sites_full.qf_supplier_name,
    qf_supplier_sites_full.qf_supplier_site_name,
    qf_supplier_sites_full.qf_supplier_site_audited_quality_types,
    qf_supplier_sites_full.qf_supplier_site_valid_audit_count,
    qf_supplier_sites_full.qf_supplier_site_last_expiration,
    qf_supplier_sites_full.qf_supplier_site_audited_requested_api,
    latest_qcr.qcr,
    latest_qcr.qcr_date
order by europe_union_usa.manufacturer_name, europe_union_usa.supplier_name, qf_supplier_sites_full.qf_supplier_name, qf_supplier_sites_full.qf_supplier_site_name, europe_union_usa.supplier_offered_api
  );