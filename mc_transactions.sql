select
    tx.vin as "VIN",
    tx.inventory_id as "Inventory ID",
    tx.opportunity_id as "Opportunity ID",
    tr.tracking_number as "Envelope Tracking Number",
    max(tr.updated_at) as "Document Received Date",
    tr.mail_group_alias as "Mail Group" --will be moved soon to envelope level
from mailcarrier.transactions tr
left join mailcarrier.transaction_xref tx
    on tx.vrm_transaction_id = tr.vrm_transaction_id
left join mailcarrier.tracking_statuses ts
    on ts.vrm_transaction_id = tr.vrm_transaction_id
where ts.status = 'DELIVERED'
group by 1, 2, 3, 4, 6
;

select
    min(f.created) as "Date Scanned",
    fx.reference_id as "Opportunity ID (dmv)" --only used for joining in QS
from "public".files f
left join "public".files_xref fx
    on fx.file_id = f.file_id
where f.file_type = 'turbo_scan'
group by 2
;

select
--max(di.created) as created_date, --if there are dups
di.machine_name as "Station",
di.created_by as "Person",
case
    when di.updated_by != 'sfp_service'
    and di.updated_by is not null
    then di.updated_by
end as "Updated By",
di.updated as "Updated Date",
dd.name as "Document Type",
ea.associated_id as "Opportunity ID (sfp)"
from document.document_instance di
left join document.document_definition dd
    on dd.document_definition_id = di.document_definition_id
left join document.envelope_association ea
    on ea.envelope_id = di.envelope_id
where ea.association_type in ('vin')
;

with
most_recent_opp as (
     select
        row_number() over(partition by vin_c order by created_date desc) as row,
        opportunity_c,
        vin_c,
        physical_title_received_date_c as physical_title_received_date,
        inbound_flip_received_date_c as inbound_flip_received_date,
        outbound_flip_sent_date_c,
        case
        when (physical_title_received_date_c > '2001-01-01 00:00:00' and outbound_flip_sent_date_c > '2001-01-01 00:00:00' and inbound_flip_received_date_c > '2001-01-01 00:00:00') then 'complete'
        when (physical_title_received_date_c > '2001-01-01 00:00:00' and outbound_flip_sent_date_c is null and inbound_flip_received_date_c is null) then 'complete'
        when (physical_title_received_date_c > '2001-01-01 00:00:00' and outbound_flip_sent_date_c is null and inbound_flip_received_date_c > '2001-01-01 00:00:00') then 'complete'
        else 'incomplete'
        end as compliance_status

    from dbt_big_db.titles_registrations
    where is_deleted != 'true'
        and record_type_id = '' --inbound 'to vroom'
        and created_date >= '2022-12-01'
        -- and vin_c = 'xxx'
),

ranking as (
    select * from most_recent_opp
    where row = 1
)

select * from ranking --(418809)
;

select
    count(vin_c),
    vin_c
    -- created_date,
    -- last_modified_date
from dbt_big_db.titles_registrations 
where record_type_id = 'xxx'
        -- and vin_c = 'xxx'
        -- and last_modified_date >= '2022-12-20'
group by 2--, 3, 4
having count(vin_c) > 1
    and max(last_modified_date) >= '2022-12-20'
-- order by vin_c --limit 500 --where opportunity_c = 'xxxx'
;

select distinct record_type_id from dbt_big_db.titles_registrations
;

select distinct name from dbt_big_db.record_types
-- where id = 'xxxx'
;

select * from dbt_big_db.titles_registrations where vin_c = 'xxxx'
