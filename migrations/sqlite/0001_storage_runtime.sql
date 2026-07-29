-- Establish the independent SQLite migration stream.
--
-- Domain tables intentionally begin with the community/auth baseline in the
-- next vertical slice. SQLx records this migration transactionally, which lets
-- the connection fixture verify migration checksums and restart behavior
-- without introducing a partial production schema.
SELECT 1;
