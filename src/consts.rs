// Values here may change based on if I feel like it
// These are modeled after the assumption of 20 mbps download speed + 200ms latency (round trip time).

pub const APP_NAME: &str = "JustAMailClient";
pub const APP_AUTHOR: &str = "Sira Tongsima";
pub const APP_VERSION: &str = "0.1.0";
pub const TEST_MAIL_DEST: &str = "Sira Tongsima <sira.tongsima@yahoo.com>";
pub const RETRIES: usize = 10;
pub const ASSUMED_DOWNLOAD_SPEED: u32 = 20 * 1024 * 1024 / 8;
pub const ASSUMED_LATENCY: u32 = 200; // ms
pub const MAX_IMAP_SESSIONS: usize = 4; // Just a hard coded limit :p
// Listfetch is the one used for listing the mailbox contents. 2^13 was chosen as the higher end size of purely text mail + < 14kb to not be bogged down with tcp slow start (reminder that the preview is the part where we will be doing searching on)
pub const MAX_LISTFETCH_SIZE: u32 = 8192; 
// Prefetch is the one used for fetching mail content that the user has a good chance of viewing (being on the same page, near the mouse, recent, etc.) 2^19 was chosen for based on the assumed download speed and latency.
// There consists of two strategies: 1) Fetching the whole mail at a time (this eats up bandwith but has the upside of reducing multiple round trip messages) 2) Fetching all parts that aren't attachments (since attachments are usually large and not viewed often)
pub const MAX_PREFETCH_STRAT1_SIZE: u32 = 524288; 
pub const MAILBOX_INBOX_ASSUMED_SIZE: u32 = 1000; // Simply for an educated guess (average is 8000 apparently 😳)
pub const NETSOCK_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);
pub const NETSOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(10); // Your internet better be quick
pub const INITIAL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(200);
pub const MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(8);
