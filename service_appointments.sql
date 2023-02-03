with appt_assigned_resources as (
    select distinct
        ser.name,
        aar.service_appointment_id,
        aar.service_resource_id
    from salesforce_prod.service_resources ser
    left join salesforce_prod.assigned_resources aar
        on aar.service_resource_id = ser.id
)

select
    del.managing_hub_facility_code_c as delivery_order_managing_hub,
    sa.appointment_number as service_appointment,
    ar.name as driver_name,
    sa.truck_number_c as service_resource_truck_id,
    sa.actual_end_time,
    sa.actual_start_time,
    case
    when del.transit_type_c = 'delivery_to_customer'
        then acc.name
    end as outbound_service_appointment_customer_name,
    case
    when outbound_service_appointment_customer_name is not null
        then sa.created_date 
    end as outbound_service_appointment_creation_date,
    case
    when outbound_service_appointment_customer_name is not null
        then sa.actual_end_time 
    end as outbound_service_appointment_completion_date,
    case
    when del.transit_type_c in ('buyback','pickup_from_customer')
        then acc.name 
    end as inbound_service_appointment_customer_name,
    case
    when inbound_service_appointment_customer_name is not null
        then sa.actual_start_time 
    end as inbound_service_appointment_pickup_date,
    del.vehicle_grade_c as vehicle_grade,
    del.transit_type_c --dlo transit type - Delivery to Customer (Outbound), Buyback or Pick Up From Customer (Inbound)
from salesforce_prod.service_appointments sa
left join appt_assigned_resources ar
    on ar.service_appointment_id = sa.id
left join salesforce_prod.work_orders wo
    on wo.id = sa.parent_record_id
left join salesforce_prod.accounts acc
    on acc.id = sa.account_id
left join salesforce_prod.delivery_orders del
    on del.id = wo.delivery_order_c
where sa.appointment_number = 'SA-3028'
    -- and sa.status = 'Completed'
    and 
    del.transit_type_c in ('delivery_to_customer','buyback','pickup_from_customer')
    and del.is_last_mile_c = true
;

select * from salesforce_prod.service_appointments where appointment_number = 'SA-3028'
;

-- Data sources:
    -- salesforce_prod.work_orders - A record specific to Field Service that tracks a customer's need to have                                           receive last-mile delivery. Each Delivery Order that has last-mile delivery                                        to customer will have a Work Order

    -- salesforce_prod.service_appointments - The scheduled delivery of the vehicle. Completion of the Service                                                   Appointment completes the Work Order (and its parent Delivery Order)                                               Service Appointments appear on the Field Service Gantt chart.

    -- salesforce_prod.service_resources - Driver. Service Resources deliver the car and fulfill Service                                                      Appointments.

    -- salesforce_prod.assigned_resources - Connects Service Resources to Service Appointments

    -- salesforce_prod.delivery_orders - A record tracking the need to move a car from A to B for a purpose


-- Create query at the granularity of Assigned Resource ID with the following columns:

    -- outbound service appointment creation date
    -- outbound service appointment completion date
    -- outbound service appointment customer name
    -- inbound service appointment pickup date
    -- inbound service appointment customer name
    -- service resource truck id number - need to confirm actual field name in Segment
    -- delivery_order.vehicle_grade_c
    -- Delivery Order Managing Hub

-- Ask #1
-- Develop logic from Salesforce data to understand when an inbound service appointment ( straight buy or buyback)
    -- actual start time is less than 2 hours from actual end time of outbound service appointment with the same
    -- truck number in the same day
    -- Same driver, same truck number, same day

-- Create the following report or additional query based off the above one to get the following metrics:

    -- Count instances when the above logic occurs and aggregate by day, week, month per managing hub
    -- Measure % of times this could have happened (Inbound shipment in the same 3 digit zip where an Outbound
        -- was completed)
    -- This can be done in Looker with a filter -- Count any wholesale graded vehicles in inbound delivery orders to hubs

-- Ask #2
-- Customer names are the same, but different SA's and/or Drivers? Did we pick up the trade-in when we dropped off the sold car.

    -- Count

    Managing Hub Name 
    Account Name 
    Transit Type 
    Delivery Order name
    Appointment Number
    --Opportunity Name 
    --Opportunity Stage 
    Vehicle 
    At Hub Date 
    Appointment Status 
    --Appt Scheduled Start 
    Appointment Actual Start 
    --Appt Scheduled End 
    Appointment Actual End 
    # Actual Duration (Minutes)
    Truck Number 
    Driver Name 
    Vehicle Grade 
