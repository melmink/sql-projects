select sum(inbound_calls)
from dbt_big_db.rpt_cj_salesforce_calls
where date > '2021-12-31'
--and inbound_calls > '0'
;

-- YTD count too high
-- select count(distinct id)
-- from salesforce_prod.tasks        
-- where task_subtype = 'Call'
--     and direction_c = 'Outbound'
--     and activity_date > '2021-12-31'
;

with outbound as (
    with calls as (
    select
        s.opportunity_id,
        s.date,
        count(distinct t.id) as outbound_calls
    from dbt_big_db.rpt_cj_salesforce_snapshot as s
    inner join dbt_big_db.tasks as t
        on t.what_id = s.opportunity_id
        and convert_timezone('UTC', 'America/new_york', t.created_date)::date = s.date
    where t.task_subtype = 'Call'
        and t.direction_c = 'Outbound'
    group by s.opportunity_id, s.date
    )

select
    s.date,
    s.opportunity_id,
    s.raw_status,
    s.delivery_date,
    s.is_delivered,
    s.derived_status,
    coalesce(c.outbound_calls, 0) as outbound_calls
from dbt_big_db.rpt_cj_salesforce_snapshot as s
left join calls as c
    on s.opportunity_id = c.opportunity_id
        and c.date = s.date
where s.date > '2021-12-31'
    and outbound_calls > 0
)

select
    sum(outbound_calls)
from outbound
;

select count(distinct name)
from dbt_big_db.sent_envelopes
where created_date > '2021-12-31'
    and sent_via_c = 'Shippo'
;

select count(id)
from dbt_big_db.documents
where created_date >'2021-12-31'
;

-- Doc count extremely low, 70
-- select *
-- from dbt_big_db.docs
-- where created > '2021-12-31'
-- ;



select count(distinct id)
from dbt_big_db.rpt_cj_salesforce_cases
where created_date > '2021-12-31'
;

select count(salesforce_id)
from dbt_big_db.rpt_cj_salesforce_payments
where created_date > '2021-12-31'
;

select count(id)
from dbt_big_db.titles_registrations
where created_date > '2021-12-31'
    and registration_and_plates_mailed_date_c is not null
;

select 
* 
from dbt_big_db.opportunities 
where created_date > '2021-12-31'
;

with lifecycle as
    (select distinct 
        o.opportunity_id_c,
        min(t.activity_date_time_c) as first_date,
        max(t.activity_date_time_c) as last_date,
        datediff(day,first_date,last_date) as lifecycle
    from dbt_big_db.opportunities o
    left join dbt_big_db.tasks t
        on t.what_id = o.id
    where t.activity_date_time_c >'2021-12-31'
        and t.task_subtype != 'Email'
    group by 1)

select 
    avg(lifecycle) 
from lifecycle
;
