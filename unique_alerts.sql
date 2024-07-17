-- This query is used to identify unique exceptions and high frequency exceptions in the logs.

WITH exception_details AS (
    SELECT
        r."span_name",
        r."message",
        r."otel_status_message",
        r.attributes->>'code.lineno' AS line_number,
        e.json_array_elements->>'event_timestamp' AS event_timestamp,
        e.json_array_elements->'attributes'->>'exception.type' AS type,
        e.json_array_elements->'attributes'->>'exception.stacktrace' AS stacktrace,
        r."start_timestamp"::timestamp
    FROM records r,
    LATERAL jsonb_array_elements(r."otel_events") AS e(json_array_elements)
    WHERE r."is_exception" = True
    AND e.json_array_elements->>'event_name' = 'exception'
),
exception_counts AS (
    SELECT
        span_name,
        message,
        otel_status_message,
        line_number,
        type,
        stacktrace,
        MIN(start_timestamp) AS first_occurrence,
        MAX(start_timestamp) AS last_occurrence,
        COUNT(*) AS occurrence_count
    FROM exception_details
    GROUP BY
        span_name,
        message,
        otel_status_message,
        line_number,
        type,
        stacktrace
),
alerts AS (
    SELECT
        type,
        line_number,
        stacktrace,
        first_occurrence,
        last_occurrence,
        occurrence_count,
        CASE
            WHEN occurrence_count > 10
            THEN 'High frequency exception'
            ELSE 'Unique exception'
        END AS alert_type
    FROM exception_counts
    WHERE
        occurrence_count > 10
        OR (last_occurrence - first_occurrence) < interval '1 hour'
)
SELECT * FROM alerts;
