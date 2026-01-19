## Cohort Retention Logic (SQL)

Cohort analysis was implemented directly in PostgreSQL using views.

Key views:
- vw_online_retail_clean
- vw_customer_first_month
- vw_cohort_activity
- vw_cohort_retention
- vw_cohort_metrics

These views define cohort assignment, retention rates, and revenue by lifecycle stage.
