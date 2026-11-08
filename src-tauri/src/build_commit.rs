/// Match the sidecar API's canonical lowercase SHA; only debug may lack Git.
pub fn is_valid(commit: &str, release: bool) -> bool {
    (!release && commit == "unknown")
        || (commit.len() == 40
            && commit
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)))
}

#[cfg(test)]
mod tests {
    use super::is_valid;

    #[test]
    fn canonical_commits_work_in_both_profiles() {
        for release in [false, true] {
            assert!(is_valid(&"abcde01234".repeat(4), release));
            for invalid in [
                "".to_owned(),
                "a".repeat(39),
                "a".repeat(41),
                "A".repeat(40),
                "g".repeat(40),
                "é".repeat(20),
            ] {
                assert!(!is_valid(&invalid, release), "accepted {invalid:?}");
            }
        }
    }

    #[test]
    fn unknown_is_reserved_for_debug_builds() {
        assert!(is_valid("unknown", false));
        assert!(!is_valid("unknown", true));
    }
}
