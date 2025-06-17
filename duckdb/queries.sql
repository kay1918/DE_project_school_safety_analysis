-- Load the dataset from CSV
CREATE OR REPLACE TABLE crime_near_schools AS
SELECT * FROM read_csv_auto('../output/*.csv');

CREATE INDEX IF NOT EXISTS idx_h3 ON crime_near_schools (crime_h3_index);

CREATE OR REPLACE VIEW crime_vs_faculty AS
SELECT
    NAME AS school_name,
    school_type,
    faculty,
    COUNT(*) AS total_crimes,
    ROUND(
        CASE
            WHEN faculty > 3 THEN COUNT(*) * 1.0 / faculty
            ELSE NULL
        END, 2
    ) AS crime_per_faculty
FROM crime_near_schools
GROUP BY school_name, school_type, faculty
ORDER BY crime_per_faculty DESC;

-- Export the final dataset to CSV format
COPY (SELECT * FROM crime_near_schools) TO '../output/final_dataset.csv' 
WITH (HEADER, DELIMITER ',');

-- Export the crime vs faculty view to CSV format
COPY (SELECT * FROM crime_vs_faculty) TO '../output/crime_vs_faculty.csv' 
WITH (HEADER, DELIMITER ',');
