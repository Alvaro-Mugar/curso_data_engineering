{% snapshot budget_snapshot_hard_deletes_newrecord %}

{{
    config(
      target_schema='snapshots',
      unique_key='_row',
      strategy='timestamp',
      updated_at='_fivetran_synced',
      hard_deletes='new_record',
    )
}}

select * from {{ source('google_sheets', 'budget') }}

{% endsnapshot %}