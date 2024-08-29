-- A. Business Report Overview
-- This report analyzes month-over-month rental data for May and June 2005, broken down by staff members. 
-- The summary report aggregates total rentals by month and staff, while the detailed report provides a breakdown of rentals per film, also categorized by month and staff.

-- 1. Fields in the Tables
-- Summary Table: Includes staff_id (smallint), formatted_month (varchar(9)), and total_rentals (int).
-- Detailed Table: Includes film_id (int), formatted_month (varchar(9)), staff_id (smallint), film_title (varchar(255)), and rental_count (int).

-- 2. Data Types
-- The fields use data types such as INT, SMALLINT, and VARCHAR.

-- 3. Source Tables
-- Data is sourced from the film, rental, staff, and inventory tables in the dvdrental database.

-- 4. Custom Transformation
-- The payment_date timestamp will be transformed into a VARCHAR(9) field using a user-defined function for clarity.

-- 5. Business Uses
-- The summary table provides a quick overview of staff productivity, while the detailed table helps in analyzing rental patterns and optimizing inventory management.

-- 6. Refresh Frequency
-- The report should be refreshed monthly to ensure data remains relevant and supports timely decision-making.

-- B. SQL Function for Transformation
CREATE OR REPLACE FUNCTION format_month(payment_date TIMESTAMP)
RETURNS VARCHAR(9)
LANGUAGE plpgsql
AS $$
DECLARE formatted_month VARCHAR(9);
BEGIN
    formatted_month := to_char(payment_date, 'Month');
    RETURN formatted_month;
END;
$$;

-- C. SQL Code for Creating Tables
-- Summary Table:
CREATE TABLE rental_summary (
    staff_id INT,
    staff_name VARCHAR(45),
    formatted_month VARCHAR(9),
    total_rentals INT,
    PRIMARY KEY(staff_id, formatted_month),
    FOREIGN KEY(staff_id) REFERENCES staff(staff_id)
);

-- Detailed Table:
CREATE TABLE rental_details (
    film_id INT,
    formatted_month VARCHAR(9),
    staff_id INT,
    staff_name VARCHAR(45),
    film_title VARCHAR(255),
    rental_count INT,
    PRIMARY KEY(film_id, formatted_month, staff_id),
    FOREIGN KEY(film_id) REFERENCES film(film_id),
    FOREIGN KEY(staff_id) REFERENCES staff(staff_id)
);

-- D. SQL Query for Extracting Raw Data
INSERT INTO rental_details
SELECT
    i.film_id,
    format_month(r.rental_date),
    r.staff_id,
    s.last_name,
    f.title,
    COUNT(r.inventory_id) AS rental_count
FROM
    rental AS r
LEFT JOIN
    inventory AS i ON r.inventory_id = i.inventory_id
INNER JOIN
    film AS f ON i.film_id = f.film_id
INNER JOIN
    staff AS s ON r.staff_id = s.staff_id
WHERE
    r.rental_date BETWEEN '2005-05-01 00:00:00' AND '2005-06-30 23:59:59'
GROUP BY
    format_month(r.rental_date),
    r.staff_id,
    s.last_name,
    i.film_id,
    f.title
ORDER BY
    format_month(r.rental_date),
    r.staff_id,
    COUNT(r.inventory_id) DESC;

-- E. SQL Code for Creating a Trigger
-- Function to update the summary table when data is added to the detailed table
CREATE OR REPLACE FUNCTION update_summary_trigger_function()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Clear the existing data in the summary table
    DELETE FROM rental_summary;

    -- Insert aggregated data into the summary table
    INSERT INTO rental_summary
    SELECT
        staff_id,
        staff_name,
        formatted_month,
        SUM(rental_count)
    FROM
        rental_details
    GROUP BY
        formatted_month,
        staff_id,
        staff_name
    ORDER BY
        formatted_month,
        staff_id;

    RETURN NEW;
END;
$$;

-- Trigger to automatically update the summary table after inserting data into the detailed table
CREATE TRIGGER refresh_summary
AFTER INSERT
ON rental_details
FOR EACH STATEMENT
EXECUTE PROCEDURE update_summary_trigger_function();

-- F. Stored Procedure for Refreshing Data
-- Procedure to refresh data in both the detailed and summary tables
CREATE OR REPLACE PROCEDURE refresh_report_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Clear the contents of the detailed table
    DELETE FROM rental_details;

    -- Populate the detailed table with fresh data
    INSERT INTO rental_details
    SELECT
        i.film_id,
        format_month(r.rental_date),
        r.staff_id,
        s.last_name,
        f.title,
        COUNT(r.inventory_id) AS rental_count
    FROM
        rental AS r
    LEFT JOIN
        inventory AS i ON r.inventory_id = i.inventory_id
    INNER JOIN
        film AS f ON i.film_id = f.film_id
    INNER JOIN
        staff AS s ON r.staff_id = s.staff_id
    WHERE
        r.rental_date BETWEEN '2005-05-01 00:00:00' AND '2005-06-30 23:59:59'
    GROUP BY
        format_month(r.rental_date),
        r.staff_id,
        s.last_name,
        i.film_id,
        f.title
    ORDER BY
        format_month(r.rental_date),
        r.staff_id,
        COUNT(r.inventory_id) DESC;

    -- The rental_summary table will be automatically cleared and refreshed by the trigger
    RETURN;
END;
$$;

-- Call the procedure to refresh both tables
CALL refresh_report_tables();

-- Verify the refreshed data
SELECT * FROM rental_summary;
SELECT * FROM rental_details;

-- F1. Job Scheduling Tool
-- We’re automating our data refresh process using pgAgent, a tool tailored for PostgreSQL. 
-- This job will run automatically on the first day of each month, executing the refresh_report_tables() procedure 
-- to update our detailed and summary tables with the latest data. This ensures our reports are always accurate and up-to-date 
-- without manual intervention, saving time and minimizing errors. We’ll monitor the job and make adjustments as needed to keep everything running smoothly.
