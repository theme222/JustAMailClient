-- Add migration script here
CREATE TABLE IF NOT EXISTS mailboxes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    create_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    update_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    account_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    mail_count INTEGER,
    recent INTEGER,
    unseen INTEGER,
    attrs BLOB, -- JSONB
    highest_modseq INTEGER,
    uid_next INTEGER,
    uid_validity INTEGER,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    UNIQUE (account_id, name)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_mailboxes_account_id ON mailboxes (account_id);

CREATE TABLE IF NOT EXISTS mailboxes_messages ( -- Many mailboxes to Many messages
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    create_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    update_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    mailbox_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    UNIQUE (mailbox_id, message_id),
    FOREIGN KEY (mailbox_id) REFERENCES mailboxes(id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS idx_mailboxes_messages_mailbox_id ON mailboxes_messages (mailbox_id);
CREATE INDEX IF NOT EXISTS idx_mailboxes_messages_message_id ON mailboxes_messages (message_id);
