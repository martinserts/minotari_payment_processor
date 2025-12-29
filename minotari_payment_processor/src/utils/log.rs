/// Masks a string (like an address) showing only start and end characters.
pub fn mask_string(s: &str) -> String {
    if s.len() <= 12 {
        return "***".to_string();
    }
    format!("{}...{}", &s[0..6], &s[s.len() - 6..])
}

/// Returns a redacted placeholder for amounts.
pub fn mask_amount(_amount: i64) -> String {
    "<REDACTED>".to_string()
}
