{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunityDonorPayment') }}
)

select
    id::bigint as donor_payment_id,
    "donorId"::bigint as donor_id,
    "campaign"::text as campaign,
    "email"::text as donor_email,
    "actualPaymentDate"::timestamp as actual_payment_date,
    "finalAmountId"::bigint as final_amount_id,
    "priceId"::bigint as price_id,
    "tipId"::bigint as tip_id,
    "gatewayOrderId"::bigint as gateway_order_id,
    "gatewayPaymentId"::bigint as gateway_payment_id,
    "gatewayPaymentStatus"::text as gateway_payment_status,
    "gatewaySubscriptionId"::text as gateway_subscription_id,
    "paymentStatus"::text as payment_status,
    "paymentType"::text as payment_type,
    "paymentMode"::text as payment_mode,
    "paymentSource"::text as payment_source,
    "medium"::text as medium,
    "period"::text as period,
    "periodInterval"::text as period_interval,
    "totalCount"::text as total_count,
    "referrer"::text as referrer,
    "source"::text as source,
    "xModifiedTimestamp"::timestamp as modified_datetime,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
