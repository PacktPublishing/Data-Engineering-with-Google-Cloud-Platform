-- Copyright 2022 Google LLC

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

--     https://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE VIEW `[your project id].dm_bikesharing.top_2_region_by_capacity`
AS
SELECT region_id, SUM(capacity) as total_capacity
FROM `[your project id].staging.stations`
WHERE region_id != ''
GROUP BY region_id
ORDER BY total_capacity desc
LIMIT 2;
