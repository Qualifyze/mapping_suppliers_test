    

  create  table
    "qualifyze_dwh"."warehouse"."public_supplier_mapping_to_qf"
    
    
    
  as (
select distinct
    hikma_union_usa_europe.source,
    hikma_union_usa_europe.manufacturer_name,
    hikma_union_usa_europe.drug_parent_name,
    hikma_union_usa_europe.administration_route,
    hikma_union_usa_europe.manufacturer_required_api,
    hikma_union_usa_europe.manufacturer_required_api_strength,
    hikma_union_usa_europe.supplier_name,
    hikma_union_usa_europe.supplier_offered_api,
    hikma_union_usa_europe.supplier_offered_api_has_same_base,
    hikma_union_usa_europe.supplier_offered_api_has_same_form,
    hikma_union_usa_europe.supplier_offered_api_is_diluted,
    qf_supplier_sites_full.qf_supplier_site_id,
    qf_supplier_sites_full.qf_supplier_name,
    qf_supplier_sites_full.qf_supplier_site_name,
    qf_supplier_sites_full.qf_supplier_site_audited_quality_types,
    qf_supplier_sites_full.qf_supplier_site_valid_audit_count,
    qf_supplier_sites_full.qf_supplier_site_last_expiration,
    qf_supplier_sites_full.qf_supplier_site_audited_requested_api,
    latest_qcr.qcr as qf_qcr,
    latest_qcr.qcr_date as qf_qcr_date,
    bool_or(coalesce(qf_supplier_sites_full.qf_is_audit_valid, '') = 'True') as qf_has_valid_audit,
    bool_or(coalesce(substance_hikma_to_qf_mapping.have_same_base, '') = 'True') as qf_has_same_base,
    bool_or(coalesce(substance_hikma_to_qf_mapping.have_same_form, '') = 'True') as qf_has_same_form,
    bool_and(coalesce(substance_hikma_to_qf_mapping.is_diluted, '') = 'True') as qf_is_diluted,
    listagg(
        coalesce(
            case
                when
                    coalesce(substance_hikma_to_qf_mapping.have_same_base, '') = 'True'
                    or coalesce(substance_hikma_to_qf_mapping.have_same_form, '') = 'True'
                    then qf_supplier_sites_full.qf_supplier_site_audited_requested_product
            end
        , '')
    , ', ') as qf_supplier_site_audited_requested_product
from raw_supplier_mapping_test.hikma_union_usa_europe
left join raw_supplier_mapping_test.qf_supplier_sites_full on
    replace(hikma_union_usa_europe.qf_supplier_site_id, '.0', '') = replace(qf_supplier_sites_full.qf_supplier_site_id, '.0', '')
left join raw_supplier_mapping_test.substance_hikma_to_qf_mapping on
    hikma_union_usa_europe.supplier_offered_api_cleaned = substance_hikma_to_qf_mapping.public_mapped_substance_cleaned
    and qf_supplier_sites_full.qf_supplier_site_audited_requested_product_cleaned = substance_hikma_to_qf_mapping.qf_mapped_substance_cleaned
    and (
         coalesce(substance_hikma_to_qf_mapping.have_same_base, '') = 'True'
         or coalesce(substance_hikma_to_qf_mapping.have_same_form, '') = 'True'
    )
left join raw_supplier_mapping_test.latest_qcr
    on replace(hikma_union_usa_europe.qf_supplier_site_id, '.0', '') = replace(latest_qcr.ceapp_id_supplier_site, '.0', '')
group by
    hikma_union_usa_europe.source,
    hikma_union_usa_europe.manufacturer_name,
    hikma_union_usa_europe.drug_parent_name,
    hikma_union_usa_europe.administration_route,
    hikma_union_usa_europe.manufacturer_required_api,
    hikma_union_usa_europe.manufacturer_required_api_strength,
    hikma_union_usa_europe.supplier_name,
    hikma_union_usa_europe.supplier_offered_api,
    hikma_union_usa_europe.supplier_offered_api_has_same_base,
    hikma_union_usa_europe.supplier_offered_api_has_same_form,
    hikma_union_usa_europe.supplier_offered_api_is_diluted,
    qf_supplier_sites_full.qf_supplier_site_id,
    qf_supplier_sites_full.qf_supplier_name,
    qf_supplier_sites_full.qf_supplier_site_name,
    qf_supplier_sites_full.qf_supplier_site_audited_quality_types,
    qf_supplier_sites_full.qf_supplier_site_valid_audit_count,
    qf_supplier_sites_full.qf_supplier_site_last_expiration,
    qf_supplier_sites_full.qf_supplier_site_audited_requested_api,
    latest_qcr.qcr,
    latest_qcr.qcr_date
order by hikma_union_usa_europe.manufacturer_name, hikma_union_usa_europe.supplier_name, qf_supplier_sites_full.qf_supplier_name, qf_supplier_sites_full.qf_supplier_site_name, hikma_union_usa_europe.supplier_offered_api
  );