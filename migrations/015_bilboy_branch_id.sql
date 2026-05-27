-- BilBoy chain mapping: one JWT in .env (BILBOY_CHAIN_TOKEN, userId=136 Yaniv,
-- exp 2027-05-27) sees all 18 chain branches via ?branches=<bilboy_branch_id>.
-- Add the column and seed the verified mapping. Per-store branches.bilboy_pass
-- stays in the schema as a fallback path; no longer collected by the UI.

ALTER TABLE branches ADD COLUMN bilboy_branch_id INTEGER;

-- 18 verified chain mappings (bilboy_branch_id ← BilBoy customerBranchId).
UPDATE branches SET bilboy_branch_id =   99 WHERE id = 9001;  -- קדיש לוז
UPDATE branches SET bilboy_branch_id = 2653 WHERE id = 9002;  -- קק"ל
UPDATE branches SET bilboy_branch_id =  106 WHERE id = 9006;  -- נווה זיו
UPDATE branches SET bilboy_branch_id =  125 WHERE id = 9007;  -- ז'בוטינסקי
UPDATE branches SET bilboy_branch_id = 3606 WHERE id = 9009;  -- שבטי ישראל
UPDATE branches SET bilboy_branch_id =  124 WHERE id = 9010;  -- שומרת
UPDATE branches SET bilboy_branch_id =  107 WHERE id = 9011;  -- ויצמן
UPDATE branches SET bilboy_branch_id =  123 WHERE id = 9012;  -- בצת
UPDATE branches SET bilboy_branch_id =  122 WHERE id = 9013;  -- לימן
UPDATE branches SET bilboy_branch_id = 3327 WHERE id = 9014;  -- קרן היסוד
UPDATE branches SET bilboy_branch_id =  483 WHERE id = 9015;  -- ההגנה
UPDATE branches SET bilboy_branch_id = 2267 WHERE id = 9016;  -- קריית טבעון
UPDATE branches SET bilboy_branch_id = 2337 WHERE id = 9017;  -- רמת השרון
UPDATE branches SET bilboy_branch_id = 3684 WHERE id = 9018;  -- דפנה
UPDATE branches SET bilboy_branch_id = 4724 WHERE id = 9019;  -- כפר סירקין
UPDATE branches SET bilboy_branch_id = 4901 WHERE id = 9020;  -- רמת גן
UPDATE branches SET bilboy_branch_id =  126 WHERE id = 126;   -- אינשטיין (Shimon)
UPDATE branches SET bilboy_branch_id =  170 WHERE id = 127;   -- תיכון / גל ודרור
