-- Histogram of over number value
WITH ResponseTimes AS (
    SELECT
        number.values[indexOf(number.names, 'message.responseTimeMS')] as response_time
    FROM logs_3
    WHERE has(number.names, 'message.responseTimeMS')
),
hist AS (
    SELECT histogram(30)(response_time) AS hist
    FROM ResponseTimes
    WHERE response_time IS NOT NULL
)
SELECT
    arrayJoin(hist).3 AS height,
    arrayJoin(hist).2 AS count,
    bar(height, 0, 80, 79) AS bar
FROM hist;


WITH ResponseTimes AS (
    SELECT
        CAST(data.message.responseTimeMS AS Float64) as response_time
    FROM logs_json
    WHERE data.message.responseTimeMS is not null
),
hist AS (
    SELECT histogram(30)(response_time) AS hist
    FROM ResponseTimes
    WHERE response_time IS NOT NULL
)
SELECT
    arrayJoin(hist).3 AS height,
    arrayJoin(hist).2 AS count,
    bar(height, 0, 80, 79) AS bar
FROM hist;

-- Count of logs per hour
SELECT
    toStartOfHour(_timestamp) as hour,
    count() as count
FROM logs_${schemaVersion}
GROUP BY hour
ORDER BY hour DESC
LIMIT 24; -- Last 24 hours


-- Count of logs per hour per method
SELECT
    string.values[indexOf(string.names, 'message.req.method')] as method,
    count() as count
FROM logs_3
WHERE has(string.names, 'message.req.method')
GROUP BY
    method;

select count(data.message.req.method), data.message.req.method from logs_json where data.message.req.method IS NOT NULL group by data.message.req.method;

-- Count of logs per hour per method
SELECT
    toStartOfHour(_timestamp) as hour,
    string.values[indexOf(string.names, 'message.req.method')] as method,
    count() as count
FROM logs_3
WHERE has(string.names, 'message.req.method')
AND _tenantId = '456'
GROUP BY
    hour,
    method
ORDER BY
    hour DESC,
    method ASC
LIMIT 100;



SELECT
    toStartOfHour(_timestamp) as hour,
    count() as count
FROM logs_3
GROUP BY hour
ORDER BY hour DESC;

SELECT
    toStartOfHour(_timestamp) as hour,
    count() as count,
    _source,
FROM logs_3
GROUP BY _source, hour
ORDER BY hour DESC;

SELECT *
FROM logs_3
WHERE has(string.names, 'message.userId')
AND string.values[indexOf(string.names, 'message.userId')] = 'e5024d6b-02b4-4725-82c9-7b6d2b77e78e'
ORDER BY _timestamp DESC;



SELECT *
FROM logs_json
WHERE data.message.userId is not null
AND data.message.userId = 'e5024d6b-02b4-4725-82c9-7b6d2b77e78e'
ORDER BY _timestamp DESC;
