with driver_day as(
    -- logic to show duration between the first 'Service Appointment Actual End' and the first 'Service Appointment Actual Start' per driver per day
    select
        sr.name,
        sa.actual_end_time::date,
        min(sa.actual_start_time) as first_sa_start, --inbound pickup
        min(sa.actual_end_time) as first_sa_end, --outbound delivery
        datediff(minute, first_sa_start, first_sa_end) as duration
    from dbt_big_db.service_appointments sa
    left join dbt_big_db.assigned_resources ar
        on ar.service_appointment_id = sa.id
    left join dbt_big_db.service_resources sr
        on sr.id = ar.service_resource_id
    where sa.actual_end_time is not null
        and sr.name is not null
    group by 1, 2
),

last_resource as (
    --logic to identify the latest resource assigned for appointments that have had more than one assigned
    select
        row_number() over(partition by sa.appointment_number order by ar.created_date desc) as row,
        sa.appointment_number,
        sr.name
    from dbt_big_db.service_appointments sa
    left join dbt_big_db.assigned_resources ar
        on ar.service_appointment_id = sa.id
    left join dbt_big_db.service_resources sr
        on sr.id = ar.service_resource_id
    where sr.name is not null
)

select
    upper(del.managing_hub_facility_code_c) as delivery_order_managing_hub,
    sa.appointment_number as service_appointment,
    lr.name as assigned_resource,
    sa.truck_number_c as service_resource_truck_id,
    case    --customer name if outbound
    when del.transit_type_c = 'delivery_to_customer'
        then acc.name
    end as outbound_service_appointment_customer_name,
    case    --null if not outbound
    when outbound_service_appointment_customer_name is not null
        then sa.created_date 
    end as outbound_service_appointment_creation_date,
    case    --null if not outbound or not complete yet
    when outbound_service_appointment_customer_name is not null
        then sa.actual_end_time 
    end as outbound_service_appointment_completion_date,
    case --customer name if inbound
    when del.transit_type_c in ('buyback','pickup_from_customer')
        then acc.name 
    end as inbound_service_appointment_customer_name,
    case    --null if not inbound or no date yet
    when inbound_service_appointment_customer_name is not null
        then sa.actual_start_time 
    end as inbound_service_appointment_pickup_date,
    left(sa.postal_code, 3) as sub_zip_code,
    del.vehicle_grade_c as vehicle_grade,
    case    --flag for when the duration between the outbound delivery and inbound pickup is less than 2 hours
    when dd.duration <= '120'
        then 1
    end as less_than_two_hours

from dbt_big_db.service_appointments sa
left join dbt_big_db.work_orders wo
    on wo.id = sa.parent_record_id
left join dbt_big_db.accounts acc
    on acc.id = sa.account_id
left join dbt_big_db.delivery_orders del
    on del.id = wo.delivery_order_c
left join dbt_big_db.assigned_resources ar
    on ar.service_appointment_id = sa.id
left join dbt_big_db.service_resources sr
    on sr.id = ar.service_resource_id
left join driver_day dd
    on dd.name = sr.name
        and dd.first_sa_start = sa.actual_start_time
left join last_resource lr
    on lr.appointment_number = sa.appointment_number
where wo.status = 'Completed'
    and del.is_last_mile_c = true
    and assigned_resource is not null
    and lr.row = 1
