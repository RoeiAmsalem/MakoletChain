-- Add Aviv's internal branch id (the value returned by /account/login as
-- branches[].id) so the chain-account multi-branch live fetch can map each
-- row in the response back to our local branch.

ALTER TABLE branches ADD COLUMN aviv_branch_id INTEGER;

-- Seed the two staging branches we already know.
UPDATE branches SET aviv_branch_id = 3 WHERE id = 126;  -- איינשטיין
UPDATE branches SET aviv_branch_id = 8 WHERE id = 127;  -- תיכון
