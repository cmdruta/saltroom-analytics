# Gold Model ERD

This Gold layer is a star schema built from the current Silver entities in `saltroom_lakehouse`.

Special key usage:
- `0 = Unknown` for all dimensions except `dim_date`
- `-1 = Online` only in `dim_staff`

Architectural notes:
- `fact_purchase` keeps `discount_code` as a degenerate dimension in the fact table.
- There is no `dim_location`.
- There is no visit-purchase bridge at this stage because visit purchase option maps directly to `dim_purchase_item`.

Fact grain:
- `fact_purchase`: one row per Silver purchase line item
- `fact_visit`: one row per Silver visit / attendance record
- `fact_timeclock`: one row per Silver timeclock row

```mermaid
erDiagram
    dim_date ||--o{ fact_purchase : purchase_date_key
    dim_date ||--o{ fact_visit : visit_date_key
    dim_date ||--o{ fact_timeclock : work_date_key

    dim_client ||--o{ fact_purchase : client_key
    dim_client ||--o{ fact_visit : client_key

    dim_service ||--o{ fact_visit : service_key

    dim_staff ||--o{ fact_purchase : sold_by_staff_key
    dim_staff ||--o{ fact_visit : staff_key
    dim_staff ||--o{ fact_visit : booked_by_staff_key
    dim_staff ||--o{ fact_timeclock : staff_key

    dim_purchase_item ||--o{ fact_purchase : purchase_item_key
    dim_purchase_item ||--o{ fact_visit : purchase_item_key

    dim_date {
        int date_key PK
        date calendar_date
        int day
        string day_name
        int day_of_week
        int week_of_year
        int month
        string month_name
        int quarter
        int year
        boolean is_weekend
    }

    dim_client {
        int client_key PK
        string silver_client_key
        string source_client_id
        string source_member_id
        string client_name
        string client_status
        date registration_date
        string zipcode
    }

    dim_service {
        int service_key PK
        string service_name
        string service_category
    }

    dim_staff {
        int staff_key PK
        string staff_name
        string staff_type
    }

    dim_purchase_item {
        int purchase_item_key PK
        string purchase_item_name
        string revenue_category
    }

    fact_purchase {
        int purchase_date_key FK
        int client_key FK
        int purchase_item_key FK
        int sold_by_staff_key FK
        string source_purchase_item_id
        decimal gross_amount
        decimal discount_amount
        decimal net_sales_amount
        decimal tax_amount
        decimal total_paid_amount
        int quantity
        string discount_code
    }

    fact_visit {
        int visit_date_key FK
        int client_key FK
        int service_key FK
        int staff_key FK
        int booked_by_staff_key FK
        int purchase_item_key FK
        string attendance_status
        string booking_source
        int visit_count
        int attended_flag
        string source_visit_id
    }

    fact_timeclock {
        int work_date_key FK
        int staff_key FK
        decimal hours_worked
        int shift_count
        string source_timeclock_record_id
    }
```
