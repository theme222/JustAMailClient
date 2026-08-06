-- Add migration script here
CREATE TABLE IF NOT EXISTS contacts ( 
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    create_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') AS INTEGER)),
    update_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') AS INTEGER)),
    name TEXT NOT NULL DEFAULT '',
    local_part TEXT NOT NULL, -- username
    domain TEXT NOT NULL,
    account_id INTEGER, -- NULLABLE
    -- UNIQUE(local_part, domain), -- Hmmmmm idk we'll see about this one
    UNIQUE (name, local_part, domain),
    FOREIGN KEY (account_id) REFERENCES accounts(id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name);
CREATE INDEX IF NOT EXISTS idx_contacts_local_part ON contacts(local_part);
CREATE INDEX IF NOT EXISTS idx_contacts_domain ON contacts(domain);