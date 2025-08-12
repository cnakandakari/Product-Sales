
select
    --fd_dw.product_material_dim.e_store,
    fd_dw.product_material_dim.material_number,
    fd_dw.product_material_dim.latest_description,
    fd_dw.product_material_dim.sku_code,
    fd_dw.product_sku_dim.product_id,
    fd_dw.product_material_dim.upc,
    fd_dw.product_material_dim.cs_upc,
    fd_dw.product_material_dim.tier1,
    fd_dw.product_material_dim.tier2,
    fd_dw.product_material_dim.tier3,
	nvl(zalts.brand_name_1,fd_dw.product_material_dim.brand_name_1) as brand_name_1,
	nvl(nvl(price.fdc_default_price, price.lic_default_price), fdx_default_price) as default_price,
	avg(nvl(zalts.material_cost,fd_dw.product_material_dim.material_cost)) as material_cost,
	nvl(fd_dw.product_merch_attributes_dim.place_holder_attribute_05,fd_dw.product_material_dim.brand_name_1) as manufacturer,
	max(decode())
    nvl(fd_dw.product_material_dim.sales_status,'00') as sales_status, -- by del type
    
    mvmt.avg_weekly_mvmt,-- by del type
    max(fd_dw.product_material_dim.vendor_primary_name) as vendor_primary_name,-- by del type
    
    min(fd_dw.product_material_dim.relative_lead_time) as relative_lead_time,-- by del type
	/*create new subquery for live*/
    sum(fd_dw.product_material_live_dim.unrestricted_use_inventory) as current_inventory,-- by del type
    avg(fd_dw.product_material_live_dim.days_on_hand_forecast) as days_on_hand_forecast,-- by del type  
    min(fd_dw.product_material_live_dim.next_po_delivery_date) as next_po_delivery_date,-- by del type
    min(fd_dw.product_material_live_dim.next_po_qty) as next_po_qty,-- by del type
    
	max(fd_dw.product_material_dim.buyer_primary_username) as buyer_primary_username,-- by del type
    fd_dw.product_material_dim.profit_center,
    fd_dw.product_material_dim.procurement_type,
    fd_dw.product_material_dim.material_type
from
    fd_dw.product_material_dim@datawpro,
    fd_dw.product_material_live_dim@datawpro,
    fd_dw.product_merch_attributes_dim@datawpro,
    fd_dw.product_sku_dim@datawpro,
    (select
        pd.material_number,
        max(case when pd.sales_org = '1400' then pd.default_price end) as fdc_default_price,
        max(case when pd.sales_org = '0001' then pd.default_price end) as lic_default_price,
        max(case when pd.sales_org in ('1300','1310') then pd.default_price end) as fdx_default_price
    from fd_dw.product_dim@datawpro pd
    where
        pd.pricing_zone_id = '0000100000'
        and pd.version_end_date >= '1-Jan-3000'
        and pd.sales_org in ('0001','1400','1300','1310')
    group by pd.material_number
    ) price,
	(select distinct
        fd_dw.product_info_dim.material_number,
		product_material_base.e_store,
        fd_dw.product_info_dim.plant,
        fd_dw.product_info_dim.sales_org,
        fd_dw.product_info_dim.distribution_channel,
        product_material_base.brand_name_1,
        fd_dw.product_material_base.material_cost * fd_dw.product_material_zalt.cs_conv as material_cost
    from 
        fd_dw.product_info_dim@datawpro,
        fd_dw.product_material_dim@datawpro product_material_base,
        fd_dw.product_material_dim@datawpro product_material_zalt
    where 
        product_material_base.material_number = fd_dw.product_info_dim.base_material_number
        and product_material_base.plant = fd_dw.product_info_dim.plant
        and product_material_base.sales_org = fd_dw.product_info_dim.sales_org
        and product_material_base.distribution_channel = fd_dw.product_info_dim.distribution_channel
        and product_material_zalt.material_number = fd_dw.product_info_dim.material_number
        and product_material_zalt.plant = fd_dw.product_info_dim.plant
        and product_material_zalt.sales_org = fd_dw.product_info_dim.sales_org
        and product_material_zalt.distribution_channel = fd_dw.product_info_dim.distribution_channel
        and fd_dw.product_info_dim.zalt_flag = 'Z-ALT'
        and fd_dw.product_info_dim.current_indicator = 'Y'
        and fd_dw.product_info_dim.sales_org in ('0001','1400','1300','1310')
		and (product_material_base.profit_department in ('Dairy','FK Dairy')
        	or product_material_base.tier1 = 'Dairy'
			or product_material_base.tier2 in ('Juice and Drinks Refrigerated','Milk Alternatives Shelf Stable'))
    ) zalts,
    (select
        material_number,
        e_store,
        round(case when nvl(fd_invoiced_weeks_l52w,0) = 0 then 0 else fd_invoiced_quantity_l52w / fd_invoiced_weeks_l52w end,0) as avg_weekly_mvmt
    from
        (select
            pid.material_number as material_number,
            ola.e_store,
            sum(case when ola.pricing_unit = 'LB' then ola.invoiced_weight else ola.invoiced_quantity end) as fd_invoiced_quantity_l52w,
            count(distinct dd.week_end_date) as fd_invoiced_weeks_l52w
        from
            fd_dw.orderline_aggr@datawpro ola,
            fd_dw.product_info_dim@datawpro pid,
            fd_dw.date_dim@datawpro dd
        where
            ola.product_info_key = pid.product_info_key     
            and ola.requested_date_key = dd.date_key
            and ola.reg_non_can_flag = 'Y'
            --and ola.e_store = 'FreshDirect'
            and dd.last_52_weeks = 'Y' 
        group by
            pid.material_number,
            ola.e_store
        )
    ) mvmt
where
    fd_dw.product_material_dim.material_number = mvmt.material_number (+)
    and fd_dw.product_material_dim.product_material_key = fd_dw.product_material_live_dim.product_material_key(+)
    and fd_dw.product_material_dim.material_number = fd_dw.product_merch_attributes_dim.material_number(+)
    and fd_dw.product_material_dim.material_number = price.material_number(+)
    and fd_dw.product_material_dim.sku_code = fd_dw.product_sku_dim.sku_code(+)
    and fd_dw.product_material_dim.plant = fd_dw.product_sku_dim.plant(+)
    and fd_dw.product_material_dim.e_store = fd_dw.product_sku_dim.e_store(+)
    and fd_dw.product_material_dim.e_store = mvmt.e_store(+)
	and fd_dw.product_material_dim.material_number = zalts.material_number(+)
    and fd_dw.product_material_dim.plant = zalts.plant(+)
    and fd_dw.product_material_dim.sales_org = zalts.sales_org(+)
	and fd_dw.product_material_dim.distribution_channel = zalts.distribution_channel(+)
    and fd_dw.product_material_dim.plant in ('1400','1300','1310')
	and (fd_dw.product_material_dim.profit_department in ('Dairy','FK Dairy')
        or fd_dw.product_material_dim.tier1 = 'Dairy'
        or fd_dw.product_material_dim.tier2 in ('Juice and Drinks Refrigerated','Milk Alternatives Shelf Stable'))

group by
    fd_dw.product_material_dim.e_store,
    fd_dw.product_material_dim.material_number,
    fd_dw.product_material_dim.latest_description,
    fd_dw.product_material_dim.sku_code,
    fd_dw.product_sku_dim.product_id,
    fd_dw.product_material_dim.upc,
    fd_dw.product_material_dim.cs_upc,
    fd_dw.product_material_dim.tier1,
    fd_dw.product_material_dim.tier2,
    fd_dw.product_material_dim.tier3,
    nvl(zalts.brand_name_1,fd_dw.product_material_dim.brand_name_1),
    nvl(fd_dw.product_material_dim.sales_status,'00'),
    case when fd_dw.product_material_dim.e_store = 'FreshDirect' then nvl(price.fdc_default_price, price.lic_default_price) 
		 else nvl(fdx_default_price,price.fdc_default_price) end,
    mvmt.avg_weekly_mvmt,
    --fd_dw.product_material_dim.vendor_primary_name,
    nvl(fd_dw.product_merch_attributes_dim.place_holder_attribute_05,fd_dw.product_material_dim.brand_name_1),
    --fd_dw.product_material_dim.buyer_primary_username,
    fd_dw.product_material_dim.procurement_type,
    fd_dw.product_material_dim.material_type,
    fd_dw.product_material_dim.profit_center
