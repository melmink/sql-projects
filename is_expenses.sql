select * 
from dbt_big_db.rpt_is_expenses
where inventoryid = '10982018' --workcompletedon > '2022-12-01'
;

--New bronze - base_is_expenses_recon.sql
select
    count(inventoryid),
    inventoryid from (

    with most_recent_exp_recon as (
        with clean_expense as (
        select
            'dms' || exp.maintid::varchar as maintid,
            exp.inventoryid,
            exp.vendorid,
            exp.typeid,
            exp.cost,
            exp.droppedoffon,
            exp.comment,
            exp.po,
            exp.mainttype,
            exp.mainttypecategoryid,
            exp.maint_category_name,
            exp.accttypecategoryid,
            exp.acct_category_name,
            exp.description,
            case
                when exp.mainttype = 'Ebay Listingal'
                    and exp.workcompletedon is null
                    then exp.droppedoffon
                else exp.workcompletedon
            end as workcompletedon
        from dbt_big_db.rpt_dms_expenses as exp
        --dbt jinja
        --from {{ ref('rpt_dms_expenses') }} as exp)
        ),

inventory_clean_expense as (
    select
        inventoryid,
        sum(cost) as total_actual_pre_advent_recon_cost
    from clean_expense
    where maint_category_name = 'Recon'
    group by 1
),

recon_cost_data as (
    select
        tasks.inventoryid,
        sum(tld.estimatedcost) as task_total_estimated_recon_cost,
        sum(tld.cost) as task_total_actual_recon_cost,
        sum(tld.estimatedhour) as task_total_estimated_recon_hours,
        sum(tld.hour) as task_total_actual_recon_hours,
        first_value((case when tasks.tasktype = 57 and (tasks.passed = 'No' or tasks.passed = 'Yes') then tasks.taskcompletedon end) ignore nulls)
        over (partition by tasks.inventoryid
            order by (case when tasks.tasktype = 57 and (tasks.passed = 'No' or tasks.passed = 'Yes') then tasks.taskcompletedon end) asc
            rows between unbounded preceding and unbounded following) as first_recon_date,
        last_value((case when tasks.tasktype = 57 and (tasks.passed = 'No' or tasks.passed = 'Yes') then tasks.taskcompletedon end) ignore nulls)
        over (partition by tasks.inventoryid
            order by (case when tasks.tasktype = 57 and (tasks.passed = 'No' or tasks.passed = 'Yes') then tasks.taskcompletedon end) asc
            rows between unbounded preceding and unbounded following) as last_recon_date
    from dbt_big_db.tbltasks as tasks
    --dbt jinja
    --from {{ source('dbt_big_db', 'tbltasks') }} as tasks
    left join "dev"."dbt_big_db"."tasktypes" as tt
    --dbt jinja
    --left join {{ source('dbt_big_db_recon', 'tasktypes') }} as tt
        on tasks.tasktype = tt.id
    left join "dev"."stitch_dbt_big_db"."tasklabordetail" as tld
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tasklabordetail') }} as tld
        on tasks.taskid = tld.taskid
    where not tld.isdeleted
        and (tt.category in ('Recon', 'Quality', 'Mechanical', 'Cosmetic', 'State Inspection', 'Info-Only', 'Inspection')
            or tasks.taskid in (56, 57)
            ) --('Recon', 'Mechanical', 'Cosmetic')
        and tld.taskid is not null
        and tasks.taskstatus not in (1, 4, 7) -- status booked, pull only costs that are confirmed with vendor - these are what is sent to Advent
        and (tld.isdeleted is null or not tld.isdeleted)
    group by tasks.inventoryid, tasks.taskid, tasks.tasktype, tt.type, tt.category, tasks.taskvendorid,
        tasks.taskenteredon, tasks.taskvendorcomment, tasks.passed, tasks.taskcompletedon
    order by 1
),

recon_aggregate_data as (
    select distinct
        tasks.inventoryid,
        first_value(taskenteredon ignore nulls)
        over (partition by tasks.inventoryid
            order by taskenteredon asc
            rows between unbounded preceding and unbounded following) as first_recon_tasked_date,
        first_value(case when tasks.tasktype = 57 then tasks.taskcompletedon end ignore nulls)
        over (partition by tasks.inventoryid
            order by (case when tasks.tasktype = 57 then tasks.taskcompletedon end) asc
            rows between unbounded preceding and unbounded following) as first_recon_date,
        last_value(case when tasks.tasktype = 57 then tasks.taskcompletedon end ignore nulls)
        over (partition by tasks.inventoryid
            order by (case when tasks.tasktype = 57 then tasks.taskcompletedon end) asc
            rows between unbounded preceding and unbounded following) as last_recon_date
    from "dev"."dbt_big_db"."tbltasks" as tasks
    --dbt jinja
    --from {{ source('dbt_big_db', 'tbltasks') }} as tasks
    left join "dev"."dbt_big_db"."tasktypes" as tt
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tasktypes') }} as tt
        on tasks.tasktype = tt.id
    where (tt.category
        in ('Recon', 'Quality', 'Mechanical', 'Cosmetic', 'State Inspection', 'Info-Only', 'Inspection')
        or tasks.taskid in (56, 57))
        and trunc(taskenteredon) >= '2019-01-01'
    group by tasks.inventoryid, tasks.tasktype, taskenteredon, tasks.taskcompletedon, tasks.passed
),

inventory_vl_costs as (
    select
        rcd.inventoryid,
        rad.first_recon_tasked_date,
        rad.first_recon_date,
        rad.last_recon_date,
        sum(task_total_estimated_recon_cost) as total_estimated_recon_cost,
        sum(task_total_actual_recon_cost) as total_vl_actual_recon_cost,
        sum(task_total_estimated_recon_hours) as total_estimated_recon_hours,
        sum(task_total_actual_recon_hours) as total_actual_recon_hours
    from recon_cost_data as rcd
    --dbt jinja
    --from recon_cost_data as rcd
    left join recon_aggregate_data as rad
    --dbt jinja
    --left join recon_aggregate_data as rad
        on rad.inventoryid = rcd.inventoryid
    group by rcd.inventoryid, rad.first_recon_tasked_date, rad.first_recon_date, rad.last_recon_date
),

ro_parts as (
    select
        a.*,
        b.typename,
        ven.name as ordered_from_vendor,
        enteredon || a.taskid || description || orderedfromvendorid || partsnotes || a.id as primary_key
    from "dev"."dbt_big_db"."tblpartsrequest" as a
    --dbt jinja
    --from {{ source('stitch_dms_tda', 'tblpartsrequest') }} as a
    left join "dev"."dbt_big_db"."tblparttype" as b
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tblparttype') }} as b
        on a.parttypeid = b.id
    left join "dev"."dbt_big_db"."tblvendor" as ven
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tblvendor') }} as ven
        on ven.vendorid = a.orderedfromvendorid
    left join "dev"."dbt_big_db"."tbltasks" as t
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tbltasks') }} as t
        on t.taskid = a.taskid
    where trunc(a.receivedon) >= '2019-04-01' and a.cancelledon is null
        and a.taskid != '4157350' -- added per Doug R.  Invalid task with incorrect costs
        and a.adventronumber != 'NO_RO'
        and a.requeststatus not in (3, 4, 5)
        and (a.quoteind is null or not a.quoteind)
        and t.taskstatus not in (4, 7, 1) -- include only parts for tasks that are started, complete, invoice approved or booked
),

inventory_ro_parts as (
    select
        tasks.inventoryid,
        sum(totalcost) as total_ro_parts_cost
    from ro_parts as rop
    left join "dev"."dbt_big_db"."tbltasks" as tasks
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tbltasks') }} as tasks
        on tasks.taskid = rop.taskid
    group by tasks.inventoryid
),

remote_recon_site_total_recon as (
    select
        inv.inventoryid,
        rr.recon_site,
        rr.actual_recon_cost
    from "dev"."dbt_big_db"."airtable_recon_all_remote" as rr
    --dbt jinja
    --from {{ source('dbt_big_db', 'airtable_recon_all_remote') }} as rr
    left join "dev"."dbt_big_db"."rpt_dms_tgt_inventory" as inv
    --dbt jinja
    --left join {{ ref('rpt_dms_tgt_inventory') }} as inv
        on inv.vin = rr.vin
            and inv.tdapurchaseddate < rr.created_time
    where actual_recon_cost is not null
        and trunc(coalesce(rqc_pass_date, zoned_date)) >= '2019-04-01'
),

vl_reported_costs as (
    select vlc.*
    from "dev"."dbt_big_db"."base_dms__cost_summary_by_inventory_id" as vlc
    --dbt jinja
    --from {{ ref('base_dms__cost_summary_by_inventory_id') }} as vlc
    left join "dev"."dbt_big_db"."rpt_dms_tgt_inventory" as inv
    --dbt jinja
    --left join {{ ref('rpt_dms_tgt_inventory') }} as inv
        on inv.inventoryid = vlc.inventoryid
    where vlc.totalcosts != 0
        and trunc(inv.tdapurchaseddate) >= '2019-04-01'
),

concessions_costs as (
    select
        tasks.inventoryid,
        sum(tld.cost) as actual_customer_concession_cost
    from "dev"."dbt_big_db"."tbltasks" as tasks
    --dbt jinja
    --from {{ source('dbt_big_db', 'tbltasks') }} as tasks
    left join "dev"."dbt_big_db"."tasktypes" as tt
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tasktypes') }} as tt
        on tasks.tasktype = tt.id
    left join "dev"."dbt_big_db"."tasklabordetail" as tld
    --dbt jinja
    --left join {{ source('dbt_big_db', 'tasklabordetail') }} as tld
        on tasks.taskid = tld.taskid
    where not tld.isdeleted
        and (not tld.isdisclosed or (tld.isdisclosed and tld.isperformwork))
        and (tt.category in ('Recon', 'Quality', 'Mechanical', 'Cosmetic', 'State Inspection', 'Info-Only', 'Inspection') or tasks.taskid in (56, 57))
        and tld.taskid is not null
        and tasks.taskvendorid != 1844
        and tt.type ilike 'We Owe -%'
    group by tasks.inventoryid, tasks.taskid, tasks.tasktype, tt.type, tt.category, tasks.taskcompletedon, tld.completedon
    order by 1, 2
)

select
    'advent_recon_' || cast(row_number() over () as varchar) as maintid,
    inv.inventoryid::int as inventoryid,
    case
        when nvl(exp.total_actual_pre_advent_recon_cost::float, 0) + -- sum recon costs found in
                nvl(vlc.totallaborcost, 0) + -- expenses and vendor lanes (incl. parts)
                nvl(vlc.totalpartscost, 0) + -- and, if not in expenses, then include
                nvl(rr.actual_recon_cost, 0) = 0 then null -- ERH Concessions (remove = 0 then null) --    remote recon site cost from airtable
        when exp.total_actual_pre_advent_recon_cost is null then
                nvl(rr.actual_recon_cost, 0) +
                nvl(exp.total_actual_pre_advent_recon_cost::float, 0) +
                nvl(vlc.totallaborcost, 0) + -- Note: airtable source to be converted to Advent
                nvl(vlc.totalpartscost, 0)
        else
                nvl(exp.total_actual_pre_advent_recon_cost::float, 0) +
                nvl(vlc.totallaborcost, 0) + -- Note: airtable source to be converted to Advent
                nvl(vlc.totalpartscost, 0)
    end as cost,
    nvl(exp.total_actual_pre_advent_recon_cost::float, 0) as dms_cost,
    nvl(vlc.totallaborcost, 0) as vl_labor_cost,
    nvl(vlc.totalpartscost, 0) as parts_manager_cost,
    nvl(rr.actual_recon_cost, 0) as remote_recon_cost,
    nvl(cc.actual_customer_concession_cost, 0) as actual_customer_concession_cost, -- ERH Concessions
    ivl.first_recon_tasked_date as droppedoffon,
    'Recon' as mainttype,
    2 as mainttypecategoryid,
    'Recon' as maint_category_name,
    ivl.last_recon_date as workcompletedon,
    ivl.first_recon_date as first_recon_date,
    vlc.totallaborcost as vlc_total_labor_cost,
    vlc.totalpartscost as vlc_total_parts_cost,
    vlc.totalcosts as vlc_total_costs,
    row_number() over(partition by inv.inventoryid::int order by ivl.last_recon_date desc) as row_order

from "dev"."dbt_big_db"."rpt_dms_tgt_inventory" as inv
        -- on inv.inventoryid = tasks.inventoryid
--dbt jinja
--from {{ ref('rpt_dms_tgt_inventory') }} as inv
left join inventory_clean_expense as exp
    on exp.inventoryid = inv.inventoryid -- Pre-Advent recon costs (incl. remote recon sites)
left join inventory_vl_costs as ivl
    on ivl.inventoryid = inv.inventoryid -- Post-Advent recon labor costs
left join inventory_ro_parts as rop
    on rop.inventoryid = inv.inventoryid -- Post-Advent recon part costs
left join remote_recon_site_total_recon as rr
    on inv.inventoryid = rr.inventoryid -- Post-Advent remote recon sites
left join vl_reported_costs as vlc
    on vlc.inventoryid = inv.inventoryid -- Costs as reported by VL cost view
left join "dev"."dbt_big_db"."stock_number_lookup" as snl
--dbt jinja
--left join {{ ref('stock_number_lookup') }} as snl
    on snl.inventoryid = inv.inventoryid
left join concessions_costs as cc
    on cc.inventoryid = inv.inventoryid
where snl.stock_number is not null
    and (nullif(exp.total_actual_pre_advent_recon_cost, 0) is not null
        or vlc.totallaborcost is not null
        or vlc.totalpartscost is not null
        or rr.actual_recon_cost is not null)
    --and inv.inventoryid = '10982018' -- for testing dedupe
)

-- ranking as (
--     select * from most_recent_exp_recon
--     where row_order = 1
-- )

select * from most_recent_exp_recon
where row_order = 1
)
group by 2
having count(inventoryid) >1
;

--New bronze - base_is_expenses_inbound_shipping.sql
select
    'advent_shipping_inbound_' || cast(row_number() over () as varchar) as maintid,
    p.inventory_id_c::int as inventoryid,
    d.carrier_c as vendorid,
    350 as typeid,
    d.delivery_cost_c::float as cost,
    d.actual_pickup_date_c as droppedoffon, -- in expense/task world, dropped of on is when vendor receives it to do the work
    d.vehicle_info_c as comment, -- there is no specific comment field, but used vehicle info for inbound shipping.
    'Inbound Shipping' as mainttype,
    1 as mainttypecategoryid,
    'SG&A' as maint_category_name,
    'Shipping' as acct_category_name,
    d.actual_delivery_date_c as workcompletedon,
    row_number() over(partition by p.inventory_id_c::int order by d.created_date desc) as row_order
from "dev"."dbt_big_db"."rpt_sfdc_delivery_orders" as d
--from {{ ref('rpt_sfdc_delivery_orders') }} as d
left join "dev"."dbt_big_db"."products" as p
--left join {{ source('dbt_big_db', 'products') }} as p
    on p.id = d.vehicle_c
left join "dev"."dbt_big_db"."rpt_dms_tgt_inventory" as inv
--left join {{ ref('rpt_dms_tgt_inventory') }} as inv
    on inv.inventoryid = p.inventory_id_c
left join "dev"."dbt_big_db"."stock_number_lookup" snl
--left join {{ ref('stock_number_lookup') }} snl
    on snl.inventoryid = inv.inventoryid
where not d.is_deleted
    and snl.stock_number is not null
    and d.type_c = 'Inbound'
    and p.inventory_id_c = '' -- for testing dedupe
;

--New bronze - base_is_expenses_outbound_shipping.sql
select
    'advent_shipping_outbound_' || cast(row_number() over () as varchar) as maintid,
    inv.inventoryid::int as inventoryid,
    352 as typeid,
    od.delivery_cost_c::float as cost,
    ad.sale_date as droppedoffon, -- Use sale date as the time when the added expenses were incurred
    'Outbound Shipping' as mainttype,
    1 as mainttypecategoryid,
    'SG&A' as maint_category_name,
    'Shipping' as acct_category_name,
    adds.description as description, -- not available in salesforce deliveries
    ad.ready_for_funding_date as workcompletedon, -- sale is closed, seems a reasonable date to use for related expenses
    row_number() over(partition by inv.inventoryid order by ad.sale_date desc) as row_order
from "dev"."dbt_big_db"."deal_adds" as adds
--from {{ source('dbt_big_db', 'deal_adds') }} as adds
left join "dev"."dbt_big_db"."deal" as ad
--left join {{ source('dbt_big_db', 'deal') }} as ad
    on ad.sale_number = adds.sale_number
        and ad.type != 4
left join "dev"."dbt_big_db"."rpt_dms_tgt_inventory" as inv
--left join {{ ref('rpt_dms_tgt_inventory') }} as inv
    on inv.adventstockid = ad.stock_number
left join "dev"."dbt_big_db"."stock_number_lookup" snl
--left join {{ ref('stock_number_lookup') }} snl
    on snl.inventoryid = inv.inventoryid
left join "dev"."dbt_big_db"."opportunities" o
--left join {{ source('salesforce_prod', 'opportunities') }} o
    on o.advent_sale_number_c = ad.sale_number
left join "dev"."dbt_big_db"."rpt_sfdc_delivery_orders" od
--left join {{ ref('rpt_sfdc_delivery_orders') }} od
    on od.opportunity_c = o.id
        and od.shipment_record_type in ('Active', 'Legacy')
where adds.code = 'SHIPPING'
    and snl.stock_number is not null
    and inv.inventoryid = '11076200' -- for testing dedupe
;
