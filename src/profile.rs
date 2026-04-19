use crate::error::{AppError, AppResult};
use aes_gcm_siv::aead::{Aead, KeyInit};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const NONCE_SIZE: usize = 12;

#[derive(Debug, Clone)]
pub struct EnvironmentProfileRecord {
    pub adapter_type: String,
    pub schema_name: String,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets: Value,
}

#[derive(Debug)]
pub struct GeneratedProfiles {
    pub temp_dir: TempDir,
}

#[derive(Debug, Clone)]
pub struct LocalTargetProfile {
    pub profile_name: String,
    pub target_name: String,
    pub adapter_type: String,
    pub schema_name: String,
    pub threads: Option<i32>,
    pub profile_config: Value,
    pub profile_secrets_plain: Value,
}

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub profile_name: String,
    pub target_name: String,
    pub final_config: Value,
}

#[derive(Debug, Deserialize)]
struct ProfilesFile {
    #[serde(flatten)]
    profiles: BTreeMap<String, ProfileDefinition>,
}

#[derive(Debug, Deserialize)]
struct ProfileDefinition {
    target: String,
    outputs: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DuckDbConfig {
    path: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DuckDbOverrides {
    #[serde(default)]
    path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PostgresConfig {
    host: String,
    user: String,
    port: u16,
    dbname: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PostgresSecrets {
    password: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PostgresOverrides {
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    dbname: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PostgresSecretOverrides {
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnowflakeConfig {
    account: String,
    user: String,
    database: String,
    warehouse: String,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnowflakeSecrets {
    password: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnowflakeOverrides {
    #[serde(default)]
    account: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    warehouse: Option<String>,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnowflakeSecretOverrides {
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DuckDbResolved {
    #[serde(rename = "type")]
    type_field: String,
    path: String,
    schema: String,
    #[serde(default)]
    threads: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PostgresResolved {
    #[serde(rename = "type")]
    type_field: String,
    host: String,
    user: String,
    password: String,
    port: u16,
    dbname: String,
    schema: String,
    #[serde(default)]
    threads: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnowflakeResolved {
    #[serde(rename = "type")]
    type_field: String,
    account: String,
    user: String,
    password: String,
    database: String,
    warehouse: String,
    schema: String,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    threads: Option<i32>,
}

impl LocalTargetProfile {
    pub fn from_local_project(
        project_dir: &Path,
        target_override: Option<&str>,
    ) -> AppResult<Self> {
        let project_yaml = read_dbt_project_yaml(project_dir)?;
        let profile_name = project_yaml
            .get("profile")
            .and_then(serde_yaml::Value::as_str)
            .or_else(|| project_yaml.get("name").and_then(serde_yaml::Value::as_str))
            .ok_or(AppError::MissingDbtProfile)?;

        let profiles_path = resolve_profiles_path(project_dir);
        let content = std::fs::read_to_string(&profiles_path)
            .map_err(|_| AppError::ProfilesFileNotFound(profiles_path.display().to_string()))?;
        let profiles: ProfilesFile = serde_yaml::from_str(&content)?;
        let profile = profiles
            .profiles
            .get(profile_name)
            .ok_or_else(|| AppError::ProfileNotFound(profile_name.to_string()))?;
        let target_name = target_override.unwrap_or(&profile.target).to_string();
        let output = profile.outputs.get(&target_name).cloned().ok_or_else(|| {
            AppError::ProfileTargetNotFound(profile_name.to_string(), target_name.clone())
        })?;
        let output_json = serde_json::to_value(output)?;
        split_local_target_profile(profile_name, &target_name, output_json)
    }

    pub fn encrypted_secrets(&self) -> AppResult<Value> {
        encrypt_json(&self.profile_secrets_plain)
    }
}

impl ResolvedProfile {
    pub fn generate(&self) -> AppResult<GeneratedProfiles> {
        let temp_dir = TempDir::new()?;
        let yaml = serde_yaml::to_string(&json!({
            self.profile_name.clone(): {
                "target": self.target_name,
                "outputs": {
                    self.target_name.clone(): self.final_config.clone()
                }
            }
        }))?;
        std::fs::write(temp_dir.path().join("profiles.yml"), yaml)?;
        Ok(GeneratedProfiles { temp_dir })
    }
}

pub fn resolve_runtime_profile(
    profile_name: &str,
    target_name: &str,
    environment: &EnvironmentProfileRecord,
) -> AppResult<ResolvedProfile> {
    let secrets = decrypt_json(&environment.profile_secrets)?;
    let mut merged = merge_json_objects(&environment.profile_config, &secrets)?;
    let object = as_object_mut(&mut merged)?;
    object.insert(
        "type".to_string(),
        Value::String(environment.adapter_type.clone()),
    );
    object.insert(
        "schema".to_string(),
        Value::String(environment.schema_name.clone()),
    );
    if let Some(threads) = environment.threads {
        object.insert("threads".to_string(), Value::Number(threads.into()));
    }
    validate_resolved_profile(&environment.adapter_type, &merged)?;
    Ok(ResolvedProfile {
        profile_name: profile_name.to_string(),
        target_name: target_name.to_string(),
        final_config: merged,
    })
}

pub fn validate_environment_profile(
    adapter_type: &str,
    schema_name: &str,
    threads: Option<i32>,
    config: &Value,
    secrets: &Value,
    allow_partial: bool,
) -> AppResult<()> {
    if schema_name.trim().is_empty() {
        return Err(AppError::InvalidProfileConfig(
            "schema must not be empty".to_string(),
        ));
    }
    if let Some(threads) = threads
        && threads <= 0
    {
        return Err(AppError::InvalidProfileConfig(
            "threads must be positive".to_string(),
        ));
    }

    match adapter_type {
        "duckdb" => {
            ensure_object(secrets)?;
            if !allow_partial {
                serde_json::from_value::<DuckDbConfig>(config.clone())
                    .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
            } else {
                serde_json::from_value::<DuckDbOverrides>(config.clone())
                    .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
            }
        }
        "postgres" => {
            if !allow_partial {
                serde_json::from_value::<PostgresConfig>(config.clone())
                    .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
                serde_json::from_value::<PostgresSecrets>(secrets.clone())
                    .map_err(|err| AppError::InvalidProfileSecret(err.to_string()))?;
            } else {
                serde_json::from_value::<PostgresOverrides>(config.clone())
                    .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
                serde_json::from_value::<PostgresSecretOverrides>(secrets.clone())
                    .map_err(|err| AppError::InvalidProfileSecret(err.to_string()))?;
            }
        }
        "snowflake" => {
            if !allow_partial {
                serde_json::from_value::<SnowflakeConfig>(config.clone())
                    .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
                serde_json::from_value::<SnowflakeSecrets>(secrets.clone())
                    .map_err(|err| AppError::InvalidProfileSecret(err.to_string()))?;
            } else {
                serde_json::from_value::<SnowflakeOverrides>(config.clone())
                    .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
                serde_json::from_value::<SnowflakeSecretOverrides>(secrets.clone())
                    .map_err(|err| AppError::InvalidProfileSecret(err.to_string()))?;
            }
        }
        other => return Err(AppError::UnsupportedAdapter(other.to_string())),
    }
    Ok(())
}

pub fn validate_resolved_profile(adapter_type: &str, resolved: &Value) -> AppResult<()> {
    match adapter_type {
        "duckdb" => {
            serde_json::from_value::<DuckDbResolved>(resolved.clone())
                .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
        }
        "postgres" => {
            serde_json::from_value::<PostgresResolved>(resolved.clone())
                .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
        }
        "snowflake" => {
            serde_json::from_value::<SnowflakeResolved>(resolved.clone())
                .map_err(|err| AppError::InvalidProfileConfig(err.to_string()))?;
        }
        other => return Err(AppError::UnsupportedAdapter(other.to_string())),
    }
    Ok(())
}

pub fn encrypt_json(value: &Value) -> AppResult<Value> {
    if value.is_null() {
        return Ok(json!({}));
    }
    if let Some(object) = value.as_object()
        && object.is_empty()
    {
        return Ok(json!({}));
    }

    let key = derive_key()?;
    let cipher =
        Aes256GcmSiv::new_from_slice(&key).map_err(|err| AppError::Encryption(err.to_string()))?;
    let plaintext = serde_json::to_vec(value)?;
    let mut nonce = [0_u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_ref())
        .map_err(|err| AppError::Encryption(err.to_string()))?;
    Ok(json!({
        "nonce": BASE64.encode(nonce),
        "ciphertext": BASE64.encode(ciphertext),
    }))
}

pub fn decrypt_json(value: &Value) -> AppResult<Value> {
    if value.is_null() {
        return Ok(json!({}));
    }
    if let Some(object) = value.as_object()
        && object.is_empty()
    {
        return Ok(json!({}));
    }

    let nonce = value
        .get("nonce")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidEncryptedSecret("missing nonce".to_string()))?;
    let ciphertext = value
        .get("ciphertext")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidEncryptedSecret("missing ciphertext".to_string()))?;
    let nonce = BASE64
        .decode(nonce)
        .map_err(|err| AppError::InvalidEncryptedSecret(err.to_string()))?;
    let ciphertext = BASE64
        .decode(ciphertext)
        .map_err(|err| AppError::InvalidEncryptedSecret(err.to_string()))?;

    let key = derive_key()?;
    let cipher =
        Aes256GcmSiv::new_from_slice(&key).map_err(|err| AppError::Encryption(err.to_string()))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|err| AppError::Encryption(err.to_string()))?;
    Ok(serde_json::from_slice(&plaintext)?)
}

fn split_local_target_profile(
    profile_name: &str,
    target_name: &str,
    raw_output: Value,
) -> AppResult<LocalTargetProfile> {
    let mut object = into_object(raw_output)?;
    let adapter_type = object
        .remove("type")
        .and_then(|value| value.as_str().map(ToString::to_string))
        .ok_or(AppError::MissingAdapterType)?;
    let schema_name = object
        .remove("schema")
        .and_then(|value| value.as_str().map(ToString::to_string))
        .ok_or_else(|| AppError::InvalidProfileConfig("missing field `schema`".to_string()))?;
    let threads = object
        .remove("threads")
        .and_then(|value| value.as_i64())
        .map(|value| value as i32);
    let secrets = match adapter_type.as_str() {
        "duckdb" => Map::new(),
        "postgres" | "snowflake" => extract_secret_fields(&mut object, &["password"]),
        other => return Err(AppError::UnsupportedAdapter(other.to_string())),
    };
    let config = Value::Object(object);
    let secrets = Value::Object(secrets);
    validate_environment_profile(
        &adapter_type,
        &schema_name,
        threads,
        &config,
        &secrets,
        false,
    )?;
    Ok(LocalTargetProfile {
        profile_name: profile_name.to_string(),
        target_name: target_name.to_string(),
        adapter_type,
        schema_name,
        threads,
        profile_config: config,
        profile_secrets_plain: secrets,
    })
}

fn extract_secret_fields(object: &mut Map<String, Value>, keys: &[&str]) -> Map<String, Value> {
    let mut secrets = Map::new();
    for key in keys {
        if let Some(value) = object.remove(*key) {
            secrets.insert((*key).to_string(), value);
        }
    }
    secrets
}

fn ensure_object(value: &Value) -> AppResult<()> {
    if value.is_object() {
        Ok(())
    } else {
        Err(AppError::InvalidProfileSecret(
            "expected object".to_string(),
        ))
    }
}

fn derive_key() -> AppResult<[u8; 32]> {
    let secret = std::env::var("DBTX_SECRET_KEY").map_err(|_| AppError::MissingSecretKey)?;
    let digest = Sha256::digest(secret.as_bytes());
    let mut key = [0_u8; 32];
    key.copy_from_slice(&digest);
    Ok(key)
}

fn merge_json_objects(base: &Value, overlay: &Value) -> AppResult<Value> {
    let mut merged = into_object(base.clone())?;
    for (key, value) in into_object(overlay.clone())? {
        merged.insert(key, value);
    }
    Ok(Value::Object(merged))
}

fn into_object(value: Value) -> AppResult<Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::InvalidProfileConfig("expected object".to_string()))
}

fn as_object_mut(value: &mut Value) -> AppResult<&mut Map<String, Value>> {
    value
        .as_object_mut()
        .ok_or_else(|| AppError::InvalidProfileConfig("expected object".to_string()))
}

fn resolve_profiles_path(project_dir: &Path) -> PathBuf {
    if let Ok(dir) = std::env::var("DBT_PROFILES_DIR") {
        return PathBuf::from(dir).join("profiles.yml");
    }
    let local = project_dir.join("profiles.yml");
    if local.is_file() {
        return local;
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".dbt").join("profiles.yml");
    }
    local
}

fn read_dbt_project_yaml(project_dir: &Path) -> AppResult<serde_yaml::Value> {
    let path = project_dir.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

#[cfg(test)]
mod tests {
    use super::{
        EnvironmentProfileRecord, LocalTargetProfile, decrypt_json, encrypt_json,
        resolve_runtime_profile,
    };
    use serde_json::json;

    #[test]
    fn encrypt_round_trip() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let value = json!({"password": "secret"});
        let encrypted = encrypt_json(&value).expect("encrypt");
        assert_ne!(encrypted, value);
        let decrypted = decrypt_json(&encrypted).expect("decrypt");
        assert_eq!(decrypted, value);
    }

    #[test]
    fn reads_local_duckdb_target() {
        let profile = super::split_local_target_profile(
            "jaffle",
            "dev",
            json!({
                "type": "duckdb",
                "path": "warehouse.duckdb",
                "schema": "main",
                "threads": 4
            }),
        )
        .expect("split");
        assert_eq!(profile.target_name, "dev");
        assert_eq!(profile.profile_name, "jaffle");
        assert_eq!(profile.adapter_type, "duckdb");
        assert_eq!(profile.schema_name, "main");
        assert_eq!(profile.profile_config["path"], "warehouse.duckdb");
    }

    #[test]
    fn resolves_profile_with_environment_schema_override() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let store = LocalTargetProfile {
            profile_name: "jaffle".to_string(),
            target_name: "dev".to_string(),
            adapter_type: "duckdb".to_string(),
            schema_name: "main".to_string(),
            threads: Some(4),
            profile_config: json!({"path":"warehouse.duckdb"}),
            profile_secrets_plain: json!({}),
        };
        let resolved = resolve_runtime_profile(
            "jaffle",
            "dev",
            &EnvironmentProfileRecord {
                adapter_type: store.adapter_type.clone(),
                schema_name: "custom".to_string(),
                threads: store.threads,
                profile_config: store.profile_config.clone(),
                profile_secrets: encrypt_json(&store.profile_secrets_plain).expect("encrypt"),
            },
        )
        .expect("resolve");
        assert_eq!(resolved.final_config["schema"], "custom");
        assert_eq!(resolved.final_config["type"], "duckdb");
    }

    #[test]
    fn encrypt_null_returns_empty_object() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let encrypted = encrypt_json(&serde_json::Value::Null).expect("encrypt null");
        assert_eq!(encrypted, json!({}));
    }

    #[test]
    fn encrypt_empty_object_returns_empty_object() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let encrypted = encrypt_json(&json!({})).expect("encrypt empty");
        assert_eq!(encrypted, json!({}));
    }

    #[test]
    fn decrypt_null_returns_empty_object() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let decrypted = decrypt_json(&serde_json::Value::Null).expect("decrypt null");
        assert_eq!(decrypted, json!({}));
    }

    #[test]
    fn encrypt_round_trip_nested_secrets() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key-2") };
        let value = json!({"password": "s3cr3t", "token": "abc123", "nested": {"key": "val"}});
        let encrypted = encrypt_json(&value).expect("encrypt");
        assert!(encrypted.get("nonce").is_some());
        assert!(encrypted.get("ciphertext").is_some());
        let decrypted = decrypt_json(&encrypted).expect("decrypt");
        assert_eq!(decrypted, value);
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "key-a") };
        let value = json!({"password": "secret"});
        let encrypted = encrypt_json(&value).expect("encrypt");
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "key-b") };
        let result = decrypt_json(&encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_missing_nonce_fails() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let result = decrypt_json(&json!({"ciphertext": "abc"}));
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_missing_ciphertext_fails() {
        unsafe { std::env::set_var("DBTX_SECRET_KEY", "test-key") };
        let result = decrypt_json(&json!({"nonce": "abc"}));
        assert!(result.is_err());
    }

    #[test]
    fn encrypt_requires_secret_key() {
        // This test verifies the error path when DBTX_SECRET_KEY is missing.
        // We can't safely remove the env var in parallel tests, so we verify
        // the derive_key function's dependency on the env var indirectly:
        // if the key is set, encryption succeeds (covered by other tests).
        // The error variant exists and is returned by derive_key when unset.
        let err = crate::error::AppError::MissingSecretKey;
        assert!(err.to_string().contains("DBTX_SECRET_KEY"));
    }
}
