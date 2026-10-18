{# Use the custom schema exactly as specified in each model's config
   (staging/intermediate/warehouse), rather than dbt's default behavior of
   concatenating it with the profile's target schema.

   PostgreSQL (default__): unchanged, schemas are staging / intermediate /
   warehouse.
   BigQuery: a schema is a dataset, and the generic names would clash with
   other work in the same Google Cloud project, so the profile's dataset is
   used as a prefix: retail_staging / retail_intermediate / retail_warehouse
   (raw data lives in retail_raw, see models/staging/sources.yml). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ return(adapter.dispatch('retail_schema_name', 'retail_dbt')(custom_schema_name, node)) }}
{%- endmacro %}

{% macro default__retail_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro bigquery__retail_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
