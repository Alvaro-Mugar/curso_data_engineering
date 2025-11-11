{% snapshot budget_snapshot_hard_deletes_invalidate %}

{{
    config(
      target_schema='snapshots',
      unique_key='_row',
      strategy='timestamp',
      updated_at='_fivetran_synced',
      hard_deletes='invalidate',
    )
}}

select * from {{ source('google_sheets', 'budget') }}

{% endsnapshot %}