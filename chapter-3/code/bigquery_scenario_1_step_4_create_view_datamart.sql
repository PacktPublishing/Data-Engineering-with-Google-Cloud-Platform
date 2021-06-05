CREATE VIEW `[your project id].dm_bikesharing.top_2_region_by_capacity`
AS
SELECT region_id, SUM(capacity) as total_capacity
FROM `[your project id].staging.stations`
WHERE region_id != ''
GROUP BY region_id
ORDER BY total_capacity desc
LIMIT 2;
