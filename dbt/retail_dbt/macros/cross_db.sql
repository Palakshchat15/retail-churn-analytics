{#
  Cross-database helpers so the same models run on PostgreSQL (target `dev`,
  the Airflow pipeline) and Google BigQuery (target `bigquery`, host only).

  Rule: every postgres__ implementation renders exactly the SQL the models used
  before these macros existed, so the PostgreSQL output is unchanged.
  default__ falls back to the PostgreSQL form.
#}


{# ---------------------------------------------------------------- casts --
   retail_cast('quantity', 'int')  ->  postgres: quantity::int
                                       bigquery: cast(quantity as int64)
   Types are written in PostgreSQL terms and mapped for BigQuery:
     int -> int64, text -> string, timestamp -> datetime (Postgres `timestamp`
     has no time zone, which is BigQuery DATETIME), date -> date,
     numeric(p, s) -> round(cast(.. as bignumeric), s) cast to numeric
                      (BigQuery rejects parameterized types in CAST; round()
                      is half away from zero, like the Postgres cast),
     bare numeric -> bignumeric (used only before a division, so the quotient
     keeps 38 decimal places before round(..., 4), close to Postgres numeric).
#}
{% macro retail_cast(expr, pg_type) -%}
    {{ return(adapter.dispatch('retail_cast', 'retail_dbt')(expr, pg_type)) }}
{%- endmacro %}

{% macro default__retail_cast(expr, pg_type) -%}
    {{ postgres__retail_cast(expr, pg_type) }}
{%- endmacro %}

{% macro postgres__retail_cast(expr, pg_type) -%}
    {{ expr }}::{{ pg_type }}
{%- endmacro %}

{% macro bigquery__retail_cast(expr, pg_type) -%}
    {%- set t = pg_type | trim | lower -%}
    {%- if t == 'int' -%}
        {%- set bq_type = 'int64' -%}
    {%- elif t == 'text' -%}
        {%- set bq_type = 'string' -%}
    {%- elif t == 'timestamp' -%}
        {%- set bq_type = 'datetime' -%}
    {%- elif t == 'date' -%}
        {%- set bq_type = 'date' -%}
    {%- elif t == 'numeric' -%}
        {%- set bq_type = 'bignumeric' -%}
    {%- elif t.startswith('numeric(') -%}
        {%- set scale = t[8:-1].split(',')[1] | trim -%}
        {{ return('cast(round(cast(' ~ expr ~ ' as bignumeric), ' ~ scale ~ ') as numeric)') }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("retail_cast: no BigQuery mapping for type '" ~ pg_type ~ "'") }}
    {%- endif -%}
    cast({{ expr }} as {{ bq_type }})
{%- endmacro %}


{# ------------------------------------------------- filtered aggregates --
   filtered_agg('max', 'o.order_date', 'o.is_sale_order')
     postgres: max(o.order_date) filter (where o.is_sale_order)
     bigquery: max(case when o.is_sale_order then o.order_date end)
   BigQuery has no FILTER clause. Rows where the condition is false or null
   become NULL and are ignored by the aggregate, which is what FILTER does.
#}
{% macro filtered_agg(func, expr, condition, distinct=false) -%}
    {{ return(adapter.dispatch('filtered_agg', 'retail_dbt')(func, expr, condition, distinct)) }}
{%- endmacro %}

{% macro default__filtered_agg(func, expr, condition, distinct=false) -%}
    {{ postgres__filtered_agg(func, expr, condition, distinct) }}
{%- endmacro %}

{% macro postgres__filtered_agg(func, expr, condition, distinct=false) -%}
    {{ func }}({{ 'distinct ' if distinct else '' }}{{ expr }}) filter (where {{ condition }})
{%- endmacro %}

{% macro bigquery__filtered_agg(func, expr, condition, distinct=false) -%}
    {{ func }}({{ 'distinct ' if distinct else '' }}case when {{ condition }} then {{ expr }} end)
{%- endmacro %}


{# ------------------------------------------------------- filtered mode --
   Most frequent value of `expr` among rows where `condition` holds.
     postgres: mode() within group (order by expr) filter (where condition)
     bigquery: no ordered-set aggregates, and an aggregate cannot feed a
               subquery in the same SELECT, so the model ranks the counts in
               its own CTE (see int_product_agg.sql) and joins the winner in
               as `mode_<expr>`; here we just pick it up per group.
   Ties: Postgres mode() returns the first tied value in collation order. The
   postgres:15 image uses en_US.utf8, which compares letters case-insensitively
   and ignores spaces/punctuation first. The BigQuery ranking breaks ties the
   same way approximately (letters and digits only, lower case, then case,
   then bytes). In this dataset 4 products have tied descriptions and this
   rule matches Postgres on all 4 (e.g. 84206B -> 'CAT WITH SUNGLASSES BLANK
   CARD', where a plain byte order would pick 'CAT W SUNGLASSES BLANK CARD').
#}
{% macro mode_where(expr, condition) -%}
    {{ return(adapter.dispatch('mode_where', 'retail_dbt')(expr, condition)) }}
{%- endmacro %}

{% macro default__mode_where(expr, condition) -%}
    {{ postgres__mode_where(expr, condition) }}
{%- endmacro %}

{% macro postgres__mode_where(expr, condition) -%}
    mode() within group (order by {{ expr }})
                filter (where {{ condition }})
{%- endmacro %}

{% macro bigquery__mode_where(expr, condition) -%}
    any_value(mode_{{ expr }})
{%- endmacro %}


{# --------------------------------------------------------- elapsed days --
   Whole days elapsed between two timestamps (end >= start).
     postgres: date_part('day', end - start)::int   (days part of the interval)
     bigquery: seconds elapsed div 86400
   Do NOT use dbt.datediff or BigQuery datetime_diff(.., day): they count
   calendar-day boundaries, so 20:00 -> 08:00 next day would be 1, while
   Postgres gives 0.
#}
{% macro elapsed_days(end_ts, start_ts) -%}
    {{ return(adapter.dispatch('elapsed_days', 'retail_dbt')(end_ts, start_ts)) }}
{%- endmacro %}

{% macro default__elapsed_days(end_ts, start_ts) -%}
    {{ postgres__elapsed_days(end_ts, start_ts) }}
{%- endmacro %}

{% macro postgres__elapsed_days(end_ts, start_ts) -%}
    date_part('day', {{ end_ts }} - {{ start_ts }})::int
{%- endmacro %}

{% macro bigquery__elapsed_days(end_ts, start_ts) -%}
    div(datetime_diff({{ end_ts }}, {{ start_ts }}, second), 86400)
{%- endmacro %}


{# ---------------------------------------------------------- month start --
     postgres: date_trunc('month', ts)::date
     bigquery: date_trunc(date(ts), month)
#}
{% macro month_start(ts) -%}
    {{ return(adapter.dispatch('month_start', 'retail_dbt')(ts)) }}
{%- endmacro %}

{% macro default__month_start(ts) -%}
    {{ postgres__month_start(ts) }}
{%- endmacro %}

{% macro postgres__month_start(ts) -%}
    date_trunc('month', {{ ts }})::date
{%- endmacro %}

{% macro bigquery__month_start(ts) -%}
    date_trunc(date({{ ts }}), month)
{%- endmacro %}


{# ------------------------------------------------ literal prefix match --
   True when `expr` starts with `prefix`, with `_` and `%` in the prefix
   matched literally.
     postgres: expr like 'GIFT\_%'   (backslash is the default LIKE escape)
     bigquery: starts_with(expr, 'GIFT_')
   (In a BigQuery string literal '\_' is not a valid escape sequence.)
#}
{% macro starts_with_literal(expr, prefix) -%}
    {{ return(adapter.dispatch('starts_with_literal', 'retail_dbt')(expr, prefix)) }}
{%- endmacro %}

{% macro default__starts_with_literal(expr, prefix) -%}
    {{ postgres__starts_with_literal(expr, prefix) }}
{%- endmacro %}

{% macro postgres__starts_with_literal(expr, prefix) -%}
    {{ expr }} like '{{ prefix | replace("\\", "\\\\") | replace("_", "\\_") | replace("%", "\\%") }}%'
{%- endmacro %}

{% macro bigquery__starts_with_literal(expr, prefix) -%}
    starts_with({{ expr }}, '{{ prefix }}')
{%- endmacro %}
