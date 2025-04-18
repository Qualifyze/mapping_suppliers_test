

with

audited_quality_types as (
    select
        audits.ceapp_id_supplier_site,
        listagg(distinct requests.quality_standard, ', ') as qf_supplier_site_audited_quality_types
    from warehouse.requests
    inner join warehouse.audits
        on requests.id_audit = audits.id_audit
            and audits.is_valid_conducted_audit
    where audits.ceapp_id_supplier_site is not null
    group by audits.ceapp_id_supplier_site
),

audit_aggregates as (
    select
        audits.ceapp_id_supplier_site,
        count(distinct audits.id_audit) as qf_supplier_site_valid_audit_count,
        count(distinct audits.id_audit) > 0 as qf_supplier_site_has_valid_audit,
        min(audits.audit_booked_date) as qf_supplier_site_first_audit_booked,
        max(audits.audit_expiration_date) as qf_supplier_site_last_expiration
    from warehouse.audits
    where audits.ceapp_id_supplier_site is not null
        and audits.is_valid_conducted_audit
    group by
        audits.ceapp_id_supplier_site
)

select distinct
    supplier_sites_ceapp.ceapp_id_supplier as qf_supplier_id,
    supplier_sites_ceapp.ceapp_id_supplier_site as qf_supplier_site_id,
    supplier_sites_ceapp.supplier_name as qf_supplier_name,
    replace(supplier_sites_ceapp.ceapp_supplier_site_name, chr(160), ' ') as qf_supplier_site_name,
    audit_aggregates.qf_supplier_site_valid_audit_count,
    audit_aggregates.qf_supplier_site_has_valid_audit,
    audited_quality_types.qf_supplier_site_audited_quality_types,
    audit_aggregates.qf_supplier_site_first_audit_booked,
    audit_aggregates.qf_supplier_site_last_expiration,
    null as qf_supplier_site_audited_requested_api,
    lower(replace(requests.request_audited_product, chr(160), ' ')) as qf_supplier_site_audited_requested_product,
    bool_or(audits.is_valid_conducted_audit and requests.is_request_operationally_valid) as qf_is_audit_valid
from warehouse.supplier_sites_ceapp
left join audit_aggregates on
    supplier_sites_ceapp.ceapp_id_supplier_site = audit_aggregates.ceapp_id_supplier_site
left join audited_quality_types on
    supplier_sites_ceapp.ceapp_id_supplier_site = audited_quality_types.ceapp_id_supplier_site
left join warehouse.audits
    on supplier_sites_ceapp.ceapp_id_supplier_site = audits.ceapp_id_supplier_site
left join warehouse.requests on
    audits.id_audit = requests.id_audit
where lower(request_audited_product) not in (
           'tbd', 'to be confirmed', 'to be defined', 'test', 'n/a', 'other', 'x',
          'tbc', 'tba', 'tdb', '-', 'a', '.', 'q', 'n', 'd', 'na', 'n.a.'
          )
group by
    supplier_sites_ceapp.ceapp_id_supplier,
    supplier_sites_ceapp.ceapp_id_supplier_site,
    supplier_sites_ceapp.supplier_name,
    supplier_sites_ceapp.ceapp_supplier_site_name,
    audit_aggregates.qf_supplier_site_valid_audit_count,
    audit_aggregates.qf_supplier_site_has_valid_audit,
    audited_quality_types.qf_supplier_site_audited_quality_types,
    audit_aggregates.qf_supplier_site_first_audit_booked,
    audit_aggregates.qf_supplier_site_last_expiration,
    lower(replace(requests.request_audited_product, chr(160), ' '))
order by qf_supplier_id, qf_supplier_site_id