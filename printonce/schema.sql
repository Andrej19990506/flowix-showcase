-- The same rules as Journal, shared by all replicas of the service.

CREATE TABLE order_label_print (
    id          bigserial   PRIMARY KEY,
    order_id    uuid        NOT NULL,
    device_id   text        NOT NULL,
    reason      text        NOT NULL,          -- auto | manual | reprint
    confirmed   boolean     NOT NULL DEFAULT false,
    printed_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX order_label_print_order ON order_label_print (order_id, printed_at DESC);

-- Claim: one transaction, serialized per order by an advisory lock, so two
-- triggers arriving in the same millisecond cannot both win.
--
--   BEGIN;
--   SELECT pg_advisory_xact_lock(hashtext($1::text));
--   SELECT 1 FROM order_label_print
--    WHERE order_id = $1
--      AND (confirmed OR printed_at > now() - interval '2 minutes')
--    LIMIT 1;
--   -- no row -> INSERT (order_id, device_id, reason) VALUES ($1, $2, 'auto');
--   COMMIT;
