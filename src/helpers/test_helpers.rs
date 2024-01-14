use tempfile::tempdir;

// Helper function to create tempoary directory for new database creation.
// This returns the path for newly created directory.
pub fn create_temp_dir() -> String {
    tempdir()
        .map(|temp_dir| temp_dir.path().to_str().unwrap_or_default().to_string())
        .unwrap_or_else(|err| {
            eprintln!("Failed to create temporary directory: {}", err);
            String::new()
        })
}
