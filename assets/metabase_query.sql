/*
-- SQL file for dashboard data monitoring
-- Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
-- Updated: 20250625
*/


-- Total Tables Monitored
SELECT COUNT(DISTINCT table_name) AS total_tables
FROM tmt_dq.dq_freshness_log;

-- % PASS Freshness SLA
SELECT 
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status = 'PASS') / NULLIF(COUNT(*), 0), 
    2
  ) AS pass_percent
FROM tmt_dq.dq_freshness_log
WHERE check_time >= NOW() - INTERVAL '7 day';

-- % PASS Completeness
SELECT 
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status = 'PASS') / NULLIF(COUNT(*), 0), 
    2
  ) AS completeness_pass_percent
FROM tmt_dq.dq_completeness_log
WHERE check_time >= NOW() - INTERVAL '7 day';

-- Total DQ Rules Evaluated
SELECT COUNT(*) AS total_rules
FROM tmt_dq.dq_rule_config;

-- Total High Severity Violations (7d)
SELECT SUM(violation_count) AS total_high_violations
FROM tmt_dq.dq_violations
WHERE severity IN ('high', 'critical') 
  AND check_time >= NOW() - INTERVAL '7 day';

-- Freshness SLA Status by Table (minutes)
SELECT 
  table_name,
  MAX(last_updated) AS max_time,
  MAX(delay_minutes) AS max_delay,
  status
FROM tmt_dq.dq_freshness_log
WHERE check_time >= NOW() - INTERVAL '7 day'
GROUP BY table_name, status
ORDER BY max_delay DESC;

-- Completeness Status - Last Check
SELECT DISTINCT ON (table_name)
  table_name,
  row_count,
  expected_min_rows,
  status,
  check_time
FROM tmt_dq.dq_completeness_log
ORDER BY table_name, check_time DESC;

-- Top Violated DQ Rules (Last 7 Days)
SELECT 
  rule_name,
  SUM(violation_count) AS total_violations,
  severity
FROM tmt_dq.dq_violations
WHERE check_time >= NOW() - INTERVAL '7 day'
GROUP BY rule_name, severity
ORDER BY total_violations DESC
LIMIT 10;

-- Violation Trend by Day
SELECT 
  DATE(check_time) AS day,
  SUM(violation_count) AS total_violations
FROM tmt_dq.dq_violations
WHERE check_time >= NOW() - INTERVAL '30 day'
GROUP BY day
ORDER BY day;

-- DQ Violations Over Time
SELECT 
  DATE(check_time) AS day,
  table_name,
  SUM(violation_count) AS total
FROM tmt_dq.dq_violations
WHERE check_time >= NOW() - INTERVAL '30 day'
GROUP BY day, table_name
ORDER BY day;

-- Outstanding High Severity Violations
SELECT 
  table_name,
  rule_name,
  violation_count,
  severity,
  check_time,
  alert_sent
FROM tmt_dq.dq_violations
WHERE severity IN ('high', 'critical')
  AND alert_sent = FALSE
ORDER BY check_time DESC;
