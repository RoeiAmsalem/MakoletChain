-- Global-salary employee type.
--
-- Until now every employee was hourly: cost = hours × hourly_rate, summed by
-- _calculate_salary_cost(). A 'global' employee instead has a FLAT monthly
-- amount the admin enters; hours are ignored entirely (no proration, full
-- amount every month they are active). Excluded from the hourly hours×rate
-- JOIN so Aviv hours can never double-count their cost.
--
--   salary_type   'hourly' (default, existing behaviour) | 'global'
--   global_salary flat monthly ₪ amount for global employees; NULL for hourly

ALTER TABLE employees ADD COLUMN salary_type TEXT DEFAULT 'hourly';
ALTER TABLE employees ADD COLUMN global_salary REAL;
