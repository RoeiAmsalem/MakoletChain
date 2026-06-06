-- Migration 028: enrich z_department_sales with profit/margin columns sourced
-- from Aviv BI report 112 (מכירות לפי מחלקות).
--
-- Background: the department breakdown source switches from the Z-902 XLS
-- section to report 112. 112 was proven to match 902's department sale amounts
-- to the cent AND to cover stores the 902 section misses (9018/9019/9016).
-- Unlike 902, report 112 also carries cost (ex-VAT), profit, profit % and
-- contribution % per department — columns the Z section never exposed.
--
-- `amount` keeps its existing meaning: sale-incl-VAT (identical to the 902
-- number), so /api/department-sales and the existing chart keep working
-- unchanged. The four new columns are nullable — old rows (902-sourced) and
-- any 112 row missing a value simply carry NULL.

ALTER TABLE z_department_sales ADD COLUMN cost_ex_vat REAL;
ALTER TABLE z_department_sales ADD COLUMN profit      REAL;
ALTER TABLE z_department_sales ADD COLUMN profit_pct  REAL;
ALTER TABLE z_department_sales ADD COLUMN contrib_pct REAL;
