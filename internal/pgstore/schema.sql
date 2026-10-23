-- Applied by pgstore.Migrate; safe to run on every start.

-- available can never go negative: even a bug in the application cannot
-- oversell, because the database rejects the transaction.
CREATE TABLE IF NOT EXISTS stock (
    sku       text PRIMARY KEY,
    available integer NOT NULL CHECK (available >= 0)
);

CREATE TABLE IF NOT EXISTS reservations (
    id         text PRIMARY KEY,
    status     text NOT NULL CHECK (status IN ('pending', 'confirmed', 'released', 'expired')),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);

-- The janitor only ever looks for overdue pending rows, so index just those.
CREATE INDEX IF NOT EXISTS reservations_pending_expiry
    ON reservations (expires_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS reservation_items (
    reservation_id text    NOT NULL REFERENCES reservations (id) ON DELETE CASCADE,
    sku            text    NOT NULL REFERENCES stock (sku),
    qty            integer NOT NULL CHECK (qty > 0),
    PRIMARY KEY (reservation_id, sku)
);
