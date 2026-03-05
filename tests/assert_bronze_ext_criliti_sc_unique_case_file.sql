-- Asserts that every case_no + _file_date combination is unique in the bronze layer.
-- Returns rows that are duplicated (test fails if any rows are returned).
SELECT
    case_no,
    _file_date,
    COUNT(*) AS cnt
FROM {{ ref('ext_criliti_sc') }}
GROUP BY case_no, _file_date
HAVING COUNT(*) > 1
