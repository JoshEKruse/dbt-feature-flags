# dbt-feature-flags

## Basics

> At a foundational level, feature flags enable code to be committed and deployed to production in a dormant state and then activated later. This gives teams more control over the user experience of the end product. Development teams can choose when and to which users new code is delivered. - Atlassian (Ian Buchannan)


More often data is being called a product. Furthermore software engineering best practices have continued to show their effectiveness in the lifecycle of data model / product development. Commits, pull requests, code reviews, merges, versioning, CI/CD, feature branches, agile sprints, etc. Today, when much of data warehousing encourages an extract, load, transform pattern, we fundamentally have more paths we can take to reach our end goal of data marts. Deferred transformation means we have almost all of the possibilities that are available to slice, dice, aggregate, and join as there can be as opposed to ETL where predefined and much less agile transformations mutate the data away from its original representation. 

This ELT pattern heavily encourages experimentation. dbt-feature-flags allow dbt developers to control SQL deployed at runtime. This allows faster iterations, faster & safer merges, and much safer experimentation. For example putting out a new v2 KPI column in a data mart behind a feature flag allows you to toggle between v1 and v2 in production without fear of regression. The same is applicable with rolling out a new `ref` to replace an old one. You could even toggle an entire experimental data mart on or off. You could put BigQuery ML models behind these flags, etc. If you "need" a data model in production but aren't confident in it, you can roll it out with the safety net of you or even a non-engineer being able to toggle it off. 

## Usage

This integration uses Harness Feature Flags. Sign up (https://harness.io/products/feature-flags)[here]. It's free to use and provides the interface for controlling your feature flags. 

Required env vars:

`DBT_FF_API_KEY` - your feature flags key

Optional:

`DBT_FF_DISABLE` - disable the patch, note that feature_flag expressions will cause your dbt models not to compile until removed or replaced. If you have the package as a dependency and aren't using it, you can save a second of initialization

`DBT_FF_DELAY` - delay before evaluating feature flags, you shouldn't need this but feature flags have a cache that is seeded asynchronously on initialization so a small delay is required to evaluate properly. Our default delay is 1s

## Examples

A contrived example:

```sql
select
    *
    {%- if feature_flag("new_relative_date_columns") %},
    case
        when current_date between fiscal_quarter_start_date and fiscal_quarter_end_date
            then 'Current'
        when current_date < fiscal_quarter_start_date then 'Future'
        when current_date > fiscal_quarter_end_date then 'Past'
    end as relative_fiscal_quarter,
    case
        when current_date between fiscal_year_start_date and fiscal_year_end_date
            then 'Current'
        when current_date < fiscal_year_start_date then 'Future'
        when current_date > fiscal_year_end_date then 'Past'
    end as relative_fiscal_year
    {% endif %}
from
    {{ ref('dim_dates__base') }}
```

BQ ML Model example (this could be ran in a run-operation, feature flags are valid anywhere dbt evaluates jinja)

```sql
CREATE OR REPLACE MODEL `bqml_tutorial.penguins_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['body_mass_g']) AS
SELECT
  *
FROM
  `bigquery-public-data.ml_datasets.penguins`
WHERE
  {% if feature_flag("penguins_model_min_weight_filter") %}
  body_mass_g > 100
  {% else %}
  body_mass_g IS NOT NULL
  {% endif %}
```

```sql
SELECT
  *
FROM
  ML.EVALUATE(
  {% if feature_flag("use_v2_ml_model") %}
  MODEL `bqml_tutorial.penguins_model_v2`,
  {% else %}
  MODEL `bqml_tutorial.penguins_model`,
  {% endif %}
    (
    SELECT
      *
    FROM
      `bigquery-public-data.ml_datasets.penguins`
    WHERE
      body_mass_g IS NOT NULL))
```

A dbt yaml example

```yaml
models:
  project:
    new_expermental_marts:
      +schema: experimental
      +enabled: "{{ feature_flag('use_new_marts') }}"

```

## Closing Remarks

Given that most of what is relevant to software is either directly or periphally relevant to data product development, we will continue to pull the description from Atlassian:

> ## Validate feature functionality
> Developers can leverage feature flags to perform “soft rollouts” of new product features. New features can be built with immediate integration of feature toggles as part of the expected release. The feature flag can be set to "off" by default so that once the code is deployed, it remains dormant during production and the new feature will be disabled until the feature toggle is explicitly activated. Teams then choose when to turn on the feature flag, which activates the code, allowing teams to perform QA and verify that it behaves as expected. If the team discovers an issue during this process, they can immediately turn off the feature flag to disable the new code and minimize user exposure to the issue.
> ## Minimize risk
> Building on the idea of soft rollouts discussed above, industrious teams can leverage feature flags in conjunction with system monitoring and metrics as a response to any observable intermittent issues. For example, if an application experiences a spike in traffic and the monitoring system reports an uptick in issues, the team may use feature flags to disable poorly performing features.
> ## Modify system behavior without disruptive changes
> Feature flags can be used to help minimize complicated code integration and deployment scenarios. Complicated new features or sensitive refactor work can be challenging to integrate into the main production branch of a repository. This is further complicated if multiple developers work on overlapping parts of the codebase. 
> Feature flags can be used to isolate new changes while known, stable code remains in place. This helps developers avoid long-running feature branches by committing frequently to the main branch of a repository behind the feature toggle. When the new code is ready there is no need for a disruptive collaborative merge and deploy scenario; the team can toggle the feature flag to enable the new system.
