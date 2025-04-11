select
    supplier_sites_ceapp.ceapp_id_supplier as qf_supplier_id,
    supplier_sites_ceapp.ceapp_id_supplier_site as qf_supplier_site_id,
    supplier_sites_ceapp.supplier_name as qf_supplier_name,
    replace(supplier_sites_ceapp.ceapp_supplier_site_name, chr(160), ' ') as qf_supplier_site_name,
from warehouse.supplier_sites_ceapp