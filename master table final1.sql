-- Step 1: Define the final Master Table Structure
CREATE TABLE master_table (
    master_record_id SERIAL PRIMARY KEY,

    -- From learner_raw
    learner_id TEXT,
    country TEXT,
    degree TEXT,
    institution TEXT,
    major TEXT,

    -- From Cognito_raw (joined via learner_id)
    user_id UUID,
    email TEXT,
    gender TEXT,
    birthdate DATE,
    usercreatedate TIMESTAMP WITH TIME ZONE,
    userlastmodifieddate TIMESTAMP WITH TIME ZONE,
    city TEXT,
    zip TEXT,
    state TEXT,

    -- From learner_opportunity_raw (joined via enrollment_id which is the actual learner_id)
    enrollment_id TEXT, -- This is the enrollment's unique ID
    assigned_cohort TEXT, -- This field acts as the FK to cohort_raw.cohort_code
    apply_date TIMESTAMP,
    status TEXT,

    -- From Opportunity_raw (joined via learner_opportunity_raw.learner_id, now named opportunity_link_id)
    -- Renamed from opportunity_id in the original schema to avoid confusion and reflect source
    opportunity_link_id CHARACTER VARYING,
    opportunity_name CHARACTER VARYING,
    category CHARACTER VARYING,
    opportunity_code CHARACTER VARYING,
    tracking_questions TEXT,

    -- From cohort_raw (joined via assigned_cohort)
    -- cohort_id (redundant 'Cohort#' field) is dropped
    cohort_code TEXT,
    start_date DATE,
    end_date DATE,
    size NUMERIC
);

-- Step 2 & 3 & 4: Extract, Transform, and Load Data into Master Table
INSERT INTO master_table (
    learner_id, country, degree, institution, major,
    user_id, email, gender, birthdate, usercreatedate, userlastmodifieddate, city, zip, state,
    enrollment_id, assigned_cohort, apply_date, status,
    opportunity_link_id, opportunity_name, category, opportunity_code, tracking_questions,
    cohort_code, start_date, end_date, size
)
WITH
    -- Cleaned learner_raw (s1) - Apply text normalization and remove duplicates
    cleaned_s1 AS (
        SELECT
            LOWER(TRIM(REPLACE(learner_id, 'Learner#', ''))) AS learner_id_cleaned,
            INITCAP(TRIM(country)) AS country,
            INITCAP(TRIM(degree)) AS degree,
            INITCAP(TRIM(institution)) AS institution,
            INITCAP(TRIM(major)) AS major,
            ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(REPLACE(learner_id, 'Learner#', ''))) ORDER BY ctid) as rn
        FROM learner_raw
        WHERE learner_id IS NOT NULL AND TRIM(CAST(learner_id AS TEXT)) != ''
    ),
    deduplicated_s1 AS (
        SELECT learner_id_cleaned AS learner_id, country, degree, institution, major
        FROM cleaned_s1 WHERE rn = 1
    ),

    -- Cleaned Cognito_raw (s6) - Deduplicate and normalize user_id
    cleaned_s6 AS (
        SELECT
            LOWER(TRIM(CAST(user_id AS TEXT)))::UUID AS user_id_cleaned,
            LOWER(TRIM(email)) AS email,
            INITCAP(TRIM(COALESCE(gender, 'rather not to say'))) AS gender, -- Gender remains specific
            birthdate::DATE AS birthdate, -- Leave NULL if source is NULL
            usercreatedate::TIMESTAMP WITH TIME ZONE AS usercreatedate,
            userlastmodifieddate::TIMESTAMP WITH TIME ZONE AS userlastmodifieddate,
            INITCAP(TRIM(city)) AS city,
            REGEXP_REPLACE(zip, '[^0-9]', '', 'g') AS zip, -- Cleaned zip
            INITCAP(TRIM(state)) AS state,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY ctid) as rn
        FROM cognito_raw
        WHERE user_id IS NOT NULL AND TRIM(CAST(user_id AS TEXT)) != ''
    ),
    deduplicated_s6 AS (
        SELECT user_id_cleaned AS user_id, email, gender, birthdate, usercreatedate, userlastmodifieddate, city, zip, state
        FROM cleaned_s6 WHERE rn = 1
    ),

    -- Cleaned learner_opportunity_raw (s5) - Deduplicate and normalize
    -- CRITICAL CHANGE: Use enrollment_id for learner_id link, and learner_id for opportunity_id link
    cleaned_s5 AS (
        SELECT
            enrollment_id, -- Keep the original enrollment_id
            LOWER(TRIM(REPLACE(enrollment_id, 'Learner#', ''))) AS learner_id_for_join,
            LOWER(TRIM(REPLACE(learner_id, 'Opportunity#', ''))) AS opportunity_id_for_join,
            INITCAP(TRIM(assigned_cohort)) AS assigned_cohort_cleaned,
            apply_date::TIMESTAMP AS apply_date, -- Leave NULL if source is NULL
            INITCAP(TRIM(status)) AS status,
            ROW_NUMBER() OVER(PARTITION BY enrollment_id, learner_id, assigned_cohort, apply_date, status ORDER BY ctid) as rn
        FROM learner_opportunity_raw
        WHERE enrollment_id IS NOT NULL AND TRIM(enrollment_id) != ''
    ),
    deduplicated_s5 AS (
        SELECT
            enrollment_id,
            learner_id_for_join,
            opportunity_id_for_join,
            assigned_cohort_cleaned AS assigned_cohort,
            apply_date,
            status
        FROM cleaned_s5 WHERE rn = 1
    ),

    -- Cleaned Opportunity_raw (s2) - Apply text normalization and calculate row number for deduplication
    cleaned_s2 AS (
        SELECT
            LOWER(TRIM(REPLACE(opportunity_id, 'Opportunity#', ''))) AS opportunity_id_cleaned,
            INITCAP(TRIM(opportunity_name)) AS opportunity_name,
            INITCAP(TRIM(category)) AS category,
            INITCAP(TRIM(opportunity_code)) AS opportunity_code,
            INITCAP(TRIM(tracking_questions)) AS tracking_questions,
            ROW_NUMBER() OVER(PARTITION BY opportunity_id ORDER BY ctid) as rn
        FROM opportunity_raw
        WHERE opportunity_id IS NOT NULL AND TRIM(CAST(opportunity_id AS TEXT)) != ''
    ),
    deduplicated_s2 AS (
        SELECT opportunity_id_cleaned AS opportunity_id, opportunity_name, category, opportunity_code, tracking_questions
        FROM cleaned_s2
        WHERE rn = 1
    ),

    -- Cleaned cohort_raw (s3) - Convert timestamps, clean text, deduplicate by cohort_code
    cleaned_s3 AS (
        SELECT
            INITCAP(TRIM(cohort_code)) AS cohort_code_cleaned,
            TO_TIMESTAMP(CAST(REPLACE(start_date, 'E+', 'e') AS NUMERIC) / 1000)::DATE AS start_date,
            TO_TIMESTAMP(CAST(REPLACE(end_date, 'E+', 'e') AS NUMERIC) / 1000)::DATE AS end_date,
            size AS size, -- Leave NULL if source is NULL for numeric field
            ROW_NUMBER() OVER(PARTITION BY cohort_code, start_date, end_date, size ORDER BY ctid) as rn
        FROM cohort_raw
        WHERE cohort_code IS NOT NULL AND TRIM(cohort_code) != ''
    ),
    deduplicated_s3 AS (
        SELECT cohort_code_cleaned AS cohort_code, start_date, end_date, size
        FROM cleaned_s3 WHERE rn = 1
    )

-- Final SELECT for INSERT: Start with all learners, then LEFT JOIN other relevant data.
-- Apply explicit CASE statements for robust NULL/empty string/literal 'Null' handling for text columns
-- Columns you want to keep as NULL if missing (enrollment_id, assigned_cohort, status, opportunity_link_id, category, cohort_code)
SELECT
    s1.learner_id,
    CASE WHEN s1.country IS NULL OR TRIM(s1.country) = '' OR LOWER(TRIM(s1.country)) = 'null' THEN 'Not Reported' ELSE s1.country END AS country,
    CASE WHEN s1.degree IS NULL OR TRIM(s1.degree) = '' OR LOWER(TRIM(s1.degree)) = 'null' THEN 'Not Reported' ELSE s1.degree END AS degree,
    CASE WHEN s1.institution IS NULL OR TRIM(s1.institution) = '' OR LOWER(TRIM(s1.institution)) = 'null' THEN 'Not Reported' ELSE s1.institution END AS institution,
    CASE WHEN s1.major IS NULL OR TRIM(s1.major) = '' OR LOWER(TRIM(s1.major)) = 'null' THEN 'Not Reported' ELSE s1.major END AS major,
    s6.user_id,
    CASE WHEN s6.email IS NULL OR TRIM(s6.email) = '' OR LOWER(TRIM(s6.email)) = 'null' THEN 'Not Reported' ELSE s6.email END AS email,
    s6.gender, -- Gender is handled specifically with 'rather not to say' in cleaned_s6
    s6.birthdate, -- Leave NULL
    s6.usercreatedate,
    s6.userlastmodifieddate,
    CASE WHEN s6.city IS NULL OR TRIM(s6.city) = '' OR LOWER(TRIM(s6.city)) = 'null' THEN 'Not Reported' ELSE s6.city END AS city,
    CASE WHEN s6.zip IS NULL OR TRIM(s6.zip) = '' OR LOWER(TRIM(s6.zip)) = 'null' THEN 'Not Reported' ELSE s6.zip END AS zip,
    CASE WHEN s6.state IS NULL OR TRIM(s6.state) = '' OR LOWER(TRIM(s6.state)) = 'null' THEN 'Not Reported' ELSE s6.state END AS state,
    CASE WHEN s5.enrollment_id IS NULL OR TRIM(s5.enrollment_id) = '' OR LOWER(TRIM(s5.enrollment_id)) = 'null' THEN NULL ELSE s5.enrollment_id END AS enrollment_id, -- Keep as NULL if missing
    CASE WHEN s5.assigned_cohort IS NULL OR TRIM(s5.assigned_cohort) = '' OR LOWER(TRIM(s5.assigned_cohort)) = 'null' THEN NULL ELSE s5.assigned_cohort END AS assigned_cohort, -- Keep as NULL if missing
    s5.apply_date, -- Leave NULL
    CASE WHEN s5.status IS NULL OR TRIM(s5.status) = '' OR LOWER(TRIM(s5.status)) = 'null' THEN NULL ELSE s5.status END AS status, -- Keep as NULL if missing
    CASE WHEN s2.opportunity_id IS NULL OR TRIM(s2.opportunity_id) = '' OR LOWER(TRIM(s2.opportunity_id)) = 'null' THEN NULL ELSE s2.opportunity_id END AS opportunity_link_id, -- Keep as NULL if missing
    CASE WHEN s2.opportunity_name IS NULL OR TRIM(s2.opportunity_name) = '' OR LOWER(TRIM(s2.opportunity_name)) = 'null' THEN 'Not Reported' ELSE s2.opportunity_name END AS opportunity_name,
    CASE WHEN s2.category IS NULL OR TRIM(s2.category) = '' OR LOWER(TRIM(s2.category)) = 'null' THEN NULL ELSE s2.category END AS category, -- Keep as NULL if missing
    CASE WHEN s2.opportunity_code IS NULL OR TRIM(s2.opportunity_code) = '' OR LOWER(TRIM(s2.opportunity_code)) = 'null' THEN NULL ELSE s2.opportunity_code END AS opportunity_code, -- Keep as NULL if missing
    CASE WHEN s2.tracking_questions IS NULL OR TRIM(s2.tracking_questions) = '' OR LOWER(TRIM(s2.tracking_questions)) = 'null' THEN 'Not Reported' ELSE s2.tracking_questions END AS tracking_questions,
    CASE WHEN s3.cohort_code IS NULL OR TRIM(s3.cohort_code) = '' OR LOWER(TRIM(s3.cohort_code)) = 'null' THEN NULL ELSE s3.cohort_code END AS cohort_code, -- Keep as NULL if missing
    s3.start_date, -- Leave NULL
    s3.end_date, -- Leave NULL
    s3.size -- Leave NULL
FROM
    deduplicated_s1 s1 -- Base: All unique learners
LEFT JOIN
    deduplicated_s6 s6 ON s1.learner_id = s6.user_id::TEXT
LEFT JOIN
    deduplicated_s5 s5 ON s1.learner_id = s5.learner_id_for_join
LEFT JOIN
    deduplicated_s3 s3 ON s5.assigned_cohort = s3.cohort_code
LEFT JOIN
    deduplicated_s2 s2 ON s5.opportunity_id_for_join = s2.opportunity_id;

-- Commit the transaction to make changes permanent
COMMIT;

