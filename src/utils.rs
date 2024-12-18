use std::collections::HashMap;

pub fn build_api_version_map() -> HashMap<i16, (i16, i16)> {
    let mut api_versions = HashMap::new();

    // Hardcode the API versions here
    // key => api_key
    // value => tuple(min_version, max_version)
    api_versions.insert(18, (0, 4));
    api_versions.insert(75, (0, 0));

    api_versions
}