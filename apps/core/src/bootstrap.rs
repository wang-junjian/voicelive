use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub(crate) struct StartupIdentity {
    pub token: String,
    #[allow(dead_code)]
    pub instance_id: Uuid,
}

#[derive(Serialize)]
struct BootstrapDocument<'a> {
    api_version: &'static str,
    instance_id: Uuid,
    url: &'a str,
    token: &'a str,
}

pub(crate) fn publish(
    runtime_dir: &Path,
    address: std::net::SocketAddr,
) -> std::io::Result<StartupIdentity> {
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random).map_err(std::io::Error::other)?;
    let token = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let identity = StartupIdentity {
        token,
        instance_id: Uuid::new_v4(),
    };
    let url = format!("http://{address}");
    let document = BootstrapDocument {
        api_version: simulmt_api::API_VERSION,
        instance_id: identity.instance_id,
        url: &url,
        token: &identity.token,
    };
    let encoded = serde_json::to_vec(&document).map_err(std::io::Error::other)?;
    let temporary = runtime_dir.join(format!(".bootstrap-{}.tmp", Uuid::new_v4()));
    let destination = bootstrap_path(runtime_dir);
    let write_result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &destination)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok::<(), std::io::Error>(())
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(identity)
}

pub(crate) fn bootstrap_path(runtime_dir: &Path) -> std::path::PathBuf {
    runtime_dir.join("bootstrap.json")
}

#[cfg(test)]
pub(crate) fn console_url(address: std::net::SocketAddr, token: &str) -> String {
    format!("http://{address}/#auth={token}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn bootstrap_contains_256_bit_token_and_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let address = "127.0.0.1:8780".parse().unwrap();
        let identity = publish(root.path(), address).unwrap();
        assert_eq!(identity.token.len(), 64);
        assert!(identity.token.bytes().all(|byte| byte.is_ascii_hexdigit()));
        let mode = std::fs::metadata(bootstrap_path(root.path()))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let contents = std::fs::read_to_string(bootstrap_path(root.path())).unwrap();
        assert!(contents.contains(&identity.token));
        assert!(!console_url(address, &identity.token).contains("?auth="));
    }
}
