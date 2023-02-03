--CJ_syndication_evcm_theoretical DONE

with partner_lookup as (
    select distinct
        r.partner_id,
        r.source_grouped,
        p.name as partner_name
    from dbt_big_db.table1 as r
    left join dbt_big_db.table2 as p
        on p.partner_id = r.partner_id
    where r.source_grouped is not null
)

select
    evcm.csv_upload_date,
    evcm.rooftop_market_code,
    evcm.source_grouped,
    evcm.sub_source,
    evcm.state,
    evcm.clean_zipcodes,
    evcm.dma,
    evcm.vin_count,
    evcm.count_of_positive_evcm,
    evcm.all_combination_count,
    evcm.base_avg_evcm,
    evcm.current_avg_evcm,
    evcm.theoretical_max_evcm,
    evcm.unplaced_negative_evcm_count,
    evcm.unplaced_neutral_evcm_count,
    evcm.unplaced_positive_evcm_count,
    evcm.unplaced_avg_evcm,
    evcm.sale_status_flag,
    evcm.vin_count * evcm.theoretical_max_evcm as total_theoretical_max_evcm,
    pl.partner_name
from dbt_big_db.table3 as evcm
left join partner_lookup as pl
    on pl.source_grouped = evcm.source_grouped
;

--QA_advent_repair_order DONE

with advent_service_data as (
    select
        ro.vin as ro_vin,
        ro.repair_order_number as ro_repair_order_number,
        ro.status as ro_status,
        ro.open_date as ro_open_date,
        ro.closed_date as ro_closed_date,
        ro.accounting_date as ro_accounting_date,
        ro.mileage_in as ro_mileage_in,
        ro.mileage_out as ro_mileage_out,
        ro.advisor as ro_advisor,
        rli.job_number as rli_job_number,
        rli.concern as rli_concern,
        rli.cause as rli_cause,
        rli.correction as rli_correction,
        rli.status as rli_status,
        rli.sale_type as rli_sale_type,
        rli.sub_sale_type as rli_sub_sale_type,
        rli.department as rli_department

    from dbt_big_db.table1 as ro
    left join dbt_big_db.table2 as rli
        on ro.repair_order_number = rli.repair_order_number
    where ro.repair_order_number not in
        (select adventronumber
         from stitch_dbt_big_db.table3
        )
)

select
    inv.inventoryid,
    ai.stock_number,
    ai.received_date as stocked_date,
    sd.*
from dbt_big_db.table1 as sd
left join dbt_big_db.table2 as ai
    on ai.vin = sd.ro_vin
    and ai.received_date <= sd.ro_open_date
left join dbt_big_db.table3 as inv
    on ai.stock_number = inv.adventstockid
order by sd.ro_vin, sd.ro_repair_order_number, sd.rli_job_number
;

--qa_inventory_snap_import_status DONE

with daily_snap_count_daily_snap_count as (
    with daily_snap_count as (
        select
            trunc(createdondate) as curr_date,
            count(*) as curr_count
        from dbt_big_db.table1
        group by trunc(createdondate)
    )

    select
        curr_date,
        curr_count,
        trunc(dateadd(day, -7, curr_date)) as prev_date
    from daily_snap_count
)

select
    a.curr_date,
    a.curr_count,
    a.prev_date,
    b.curr_count as prev_count,
    (cast(a.curr_count as float) / cast(b.curr_count as float)) as ratio
from daily_snap_count_daily_snap_count as a
left join daily_snap_count_daily_snap_count as b
    on b.curr_date = a.prev_date
order by a.curr_date desc
;

--QA_parts_accrual CANCELED

-- with first_download as (
--     select
--         invoicefileid,
--         min(datedownloaded) as firstdownloaddate
--     from dbt_big_db.table1
--     group by invoicefileid
-- ),

-- part_credits as (
--     select
--         partrequestid,
--         sum(amount) as total_credit
--     from stitchdbt_big_db.table2
--     group by partrequestid
-- )

-- select
--     cast(r.id as varchar(10)) as document_number,
--     r.taskid,
--     tv.name as task_vendor,
--     case when r.parttypeid = 1 then v.name else 'ADVANCE AUTO PARTS INC' end as vendor_name,
--     t.taskfirstcompletedon as task_first_completed_on,
--     r.senttobookingon as approved_date,
--     r.bookedon as booked_date,
--     rs.vin,
--     inv.adventstockid,
--     sum(r.totalcost) as total_cost,
--     sum(coalesce(r.totalcost + c.total_credit, r.totalcost)) as final_cost,
--     r.partvendorinvoicenumber as part_vendor_invoice_number,
--     r.cancelledon as cancelled_on,
--     t.taskstatus as task_status,
--     fd.firstdownloaddate as first_download_date,
--     r.receivedon as received_on

-- from stitch_dbt_big_db.table3 as r
-- inner join stitch_dbt_big_db.table4 as t
--      on r.taskid = t.taskid
-- inner join stitch_dbt_big_db.table5 as rs
--      on t.inventoryid = rs.inventoryid
-- inner join dbt_big_db.table6 as inv
--      on inv.inventoryid = t.inventoryid
-- left join part_credits as c
--      on c.partrequestid = r.id
-- left join stitch_dbt_big_db.table1 as v
--      on r.orderedfromvendorid = v.vendorid
-- left join stitch_dbt_big_db.table2 as tv
--      on t.taskvendorid = tv.vendorid
-- left join first_download as fd
--      on r.invoicefileid = fd.invoicefileid

-- where r.requeststatus not in (3, 4, 5)
--     and t.taskstatus = 3
--     and r.quoteind = 0
--     and (r.adventronumber = '' or r.adventronumber is null)
--     and r.totalcost > 0

-- group by
--     cast(r.id as varchar(10)),
--     r.taskid,
--     case when r.parttypeid = 1 then v.name else 'ADVANCE AUTO PARTS INC' end,
--     t.taskfirstcompletedon,
--     r.senttobookingon,
--     r.bookedon,
--     rs.vin,
--     inv.adventstockid,
--     r.partvendorinvoicenumber,
--     r.cancelledon,
--     t.taskstatus,
--     r.bookedon,
--     fd.firstdownloaddate,
--     tv.name,
--     r.receivedon

-- order by t.taskfirstcompletedon
--;

--QA_tgt_inventory

select
    inventoryid,
    count(*) as count
from dbt_big_db.table1
group by 1
;

--QA_VL_booked_tasks

select
    inv.vin,
    tblt.taskowner,
    tblt.taskdescription,
    tblt.tasktype,
    tblt.taskstatus,
    tblt.taskenteredon,
    tblt.taskcompletedon,
    tblt.inventoryid,
    tblt.taskid,
    tblt.taskstartedon,
    tblt.taskvendorid,
    tblt.priority,
    tblt.taskreadyforpickupon,
    tblt.taskvendorstatus,
    tblt.taskvendorcomment,
    tblt.taskvendorapprovedon,
    null as taskvendorwaitingonapprovaldate,
    null as taskapprovalcomment,
    null as taskapprovedby,
    tblt.taskestimatedreadytime,
    null as parenttaskid,
    tblt.estimatedcost,
    tblt.resourceid,
    tblt.estimatedhours,
    tblt.inttaskerid,
    tblt.exttaskerid,
    tblt.enteredby,
    tblt.passed,
    tblt.reasoncodeid,
    tblt.adventronumber,
    tblt.bookdate,
    tblt.senttobookingon,
    tt.type as task_name,
    tt.category as recon_type,
    vendor.name as vendor_name

from stitch_dbt_big_db.table1 as tblt
left join stitch_dbt_big_db.table2 as tt
    on tblt.tasktype = tt.id
left join dbt_big_db.table3 as inv
    on inv.inventoryid = tblt.inventoryid
left join stitch_dbt_big_db.table4 as vendor
    on vendor.vendorid = tblt.taskvendorid
left join stitch_dbt_big_db.table5 as cro
    on cro.adventronumber = tblt.adventronumber
where tblt.taskstatus = 10
    and tblt.taskvendorid != 1844    -- partial fix for task records showning inventoryids of vehicles purchased in distant past
    and tblt.adventronumber is not null
    and tblt.adventronumber not in ('', 'NO_RO', 'CANCEL_PART', 'NO_STOCK_ID', 'NO_COST', 'OLD_ACCIDENT' )
    and cro.adventronumber is null

;
