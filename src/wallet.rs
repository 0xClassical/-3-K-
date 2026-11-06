//! Local-only signing. This module has no RPC client and cannot broadcast.
//! V2 keystore: Argon2id (64 MiB / 3 passes), random salt, AES-256-GCM.
use std::io::Write;
use std::path::Path;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};
use k256::ecdsa::SigningKey;
use rlp::RlpStream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tiny_keccak::{Hasher, Keccak};
use zeroize::Zeroizing;

const AAD: &[u8] = b"meme-quant-wallet-v2|argon2id-m65536-t3-p1";
pub const CHAIN_ID: u64 = 56;

pub struct Wallet {
    address: [u8; 20],
    signing_key: SigningKey,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    version: u8,
    kdf: String,
    salt: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug)]
pub struct WalletError(pub String);
impl std::fmt::Display for WalletError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for WalletError {}

#[derive(Clone)]
pub struct Transaction {
    pub nonce: u64,
    pub gas_price: u128,
    pub gas_limit: u64,
    pub to: [u8; 20],
    pub value: u128,
    pub data: Vec<u8>,
}

pub struct SignedTransaction {
    pub raw: String,
    pub hash: String,
}

impl Wallet {
    #[cfg(test)]
    pub(crate) fn test_wallet() -> Self {
        Self::from_private_key(&[1; 32]).unwrap()
    }
    pub fn load_encrypted(path: &Path, password: &str) -> Result<Self, WalletError> {
        let content = std::fs::read_to_string(path).map_err(err)?;
        Self::from_private_key(&decrypt_v2(&content, password)?)
    }

    fn from_private_key(pk: &[u8]) -> Result<Self, WalletError> {
        let signing_key = SigningKey::from_slice(pk).map_err(err)?;
        let point = signing_key.verifying_key().to_encoded_point(false);
        let hash = keccak256(&point.as_bytes()[1..]);
        let mut address = [0; 20];
        address.copy_from_slice(&hash[12..]);
        Ok(Self {
            address,
            signing_key,
        })
    }

    pub fn address(&self) -> String {
        format!("0x{}", hex::encode(self.address))
    }

    /// No nonce allocation or networking here. Integer RLP fields must be canonical.
    pub fn sign(&self, tx: &Transaction) -> Result<SignedTransaction, WalletError> {
        if tx.gas_price == 0 || tx.gas_limit < 21_000 || tx.value == 0 || tx.data.len() < 4 {
            return Err(WalletError("invalid transaction".into()));
        }
        let mut unsigned = RlpStream::new_list(9);
        append_core(&mut unsigned, tx);
        unsigned.append(&CHAIN_ID).append(&0u8).append(&0u8);
        let digest = keccak256(&unsigned.out());
        let (signature, recovery) = self
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(err)?;
        if recovery.to_byte() > 1 {
            return Err(WalletError("unsupported recovery id".into()));
        }
        let mut signed = RlpStream::new_list(9);
        append_core(&mut signed, tx);
        signed.append(&(CHAIN_ID * 2 + 35 + recovery.to_byte() as u64));
        let r = signature.r().to_bytes();
        let s = signature.s().to_bytes();
        signed.append(&trim_integer(&r)).append(&trim_integer(&s));
        let bytes = signed.out();
        Ok(SignedTransaction {
            hash: format!("0x{}", hex::encode(keccak256(&bytes))),
            raw: format!("0x{}", hex::encode(bytes)),
        })
    }
}

fn append_core(rlp: &mut RlpStream, tx: &Transaction) {
    rlp.append(&tx.nonce)
        .append(&tx.gas_price)
        .append(&tx.gas_limit)
        .append(&tx.to.as_slice())
        .append(&tx.value)
        .append(&tx.data);
}
fn trim_integer(value: &[u8]) -> &[u8] {
    &value[value.iter().position(|b| *b != 0).unwrap_or(value.len())..]
}
pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak::v256();
    hasher.update(data);
    let mut out = [0; 32];
    hasher.finalize(&mut out);
    out
}
fn err(e: impl std::fmt::Display) -> WalletError {
    WalletError(e.to_string())
}

fn derive(password: &str, salt: &[u8]) -> Result<Zeroizing<[u8; 32]>, WalletError> {
    if password.len() > 1024 {
        return Err(WalletError("password too long".into()));
    }
    let params = Params::new(65_536, 3, 1, Some(32)).map_err(err)?;
    let mut memory = Zeroizing::new(vec![argon2::Block::default(); params.block_count()]);
    let mut key = Zeroizing::new([0; 32]);
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into_with_memory(password.as_bytes(), salt, &mut *key, &mut *memory)
        .map_err(err)?;
    Ok(key)
}
fn encrypt_v2(pk: &[u8], password: &str) -> Result<String, WalletError> {
    if password.chars().count() < 12 {
        return Err(WalletError("密码至少 12 个字符".into()));
    }
    Wallet::from_private_key(pk)?;
    let salt = Aes256Gcm::generate_nonce(&mut OsRng);
    // Two independently random nonce samples produce a 16-byte salt.
    let more = Aes256Gcm::generate_nonce(&mut OsRng);
    let mut salt_bytes = [0; 16];
    salt_bytes[..12].copy_from_slice(&salt);
    salt_bytes[12..].copy_from_slice(&more[..4]);
    let key = derive(password, &salt_bytes)?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(err)?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, Payload { msg: pk, aad: AAD })
        .map_err(|_| WalletError("encryption failed".into()))?;
    serde_json::to_string_pretty(&KeyFile {
        version: 2,
        kdf: "argon2id-m65536-t3-p1".into(),
        salt: hex::encode(salt_bytes),
        nonce: hex::encode(nonce),
        ciphertext: hex::encode(ciphertext),
    })
    .map_err(err)
}
fn decrypt_v2(content: &str, password: &str) -> Result<Zeroizing<Vec<u8>>, WalletError> {
    if content.len() > 2048 {
        return Err(WalletError("keystore too large".into()));
    }
    let file: KeyFile = serde_json::from_str(content).map_err(|_| {
        WalletError("只接受 V2 密钥文件；旧文件请用 --migrate-wallet 离线迁移".into())
    })?;
    if file.version != 2 || file.kdf != "argon2id-m65536-t3-p1" {
        return Err(WalletError("unsupported keystore version/KDF".into()));
    }
    let salt = hex::decode(file.salt).map_err(err)?;
    let nonce = hex::decode(file.nonce).map_err(err)?;
    let ciphertext = hex::decode(file.ciphertext).map_err(err)?;
    if salt.len() != 16 || nonce.len() != 12 || ciphertext.len() != 48 {
        return Err(WalletError("invalid keystore lengths".into()));
    }
    let key = derive(password, &salt)?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(err)?;
    let pk = Zeroizing::new(
        cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: AAD,
                },
            )
            .map_err(|_| WalletError("密码错误或文件损坏".into()))?,
    );
    Wallet::from_private_key(&pk)?;
    Ok(pk)
}

/// Offline import: verify the selected public account BEFORE creating any file.
/// Neither the private key nor the password is returned or logged.
pub fn encrypt_private_key_checked(
    pk_hex: &str,
    password: &str,
    path: &Path,
    expected_address: Option<&str>,
) -> Result<String, WalletError> {
    let (address, encrypted) = prepare_keystore(pk_hex, password, expected_address)?;
    write_new_keystore(path, &encrypted)?;
    Ok(address)
}

fn prepare_keystore(
    pk_hex: &str,
    password: &str,
    expected_address: Option<&str>,
) -> Result<(String, String), WalletError> {
    let pk = Zeroizing::new(hex::decode(pk_hex.trim().trim_start_matches("0x")).map_err(err)?);
    let address = Wallet::from_private_key(&pk)?.address();
    if let Some(expected) = expected_address {
        let hex = expected.strip_prefix("0x").unwrap_or("");
        if hex.len() != 40 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(WalletError(
                "预期地址必须是 0x 开头的 40 位十六进制账户地址".into(),
            ));
        }
        if !address.eq_ignore_ascii_case(expected) {
            return Err(WalletError(
                "账户地址不匹配：未创建钱包文件，请核对所选账户".into(),
            ));
        }
    }
    Ok((address, encrypt_v2(&pk, password)?))
}

/// Explicit offline migration only; never used by automatic startup/unlock.
pub fn migrate_legacy(
    input: &Path,
    old_password: &str,
    new_password: &str,
    output: &Path,
) -> Result<(), WalletError> {
    let text = std::fs::read_to_string(input).map_err(err)?;
    let bytes = hex::decode(text.trim()).map_err(err)?;
    if bytes.len() != 60 {
        return Err(WalletError("invalid legacy keystore".into()));
    }
    let key = Zeroizing::new(<[u8; 32]>::from(Sha256::digest(old_password.as_bytes())));
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(err)?;
    let pk = Zeroizing::new(
        cipher
            .decrypt(Nonce::from_slice(&bytes[..12]), &bytes[12..])
            .map_err(|_| WalletError("旧密码错误或文件损坏".into()))?,
    );
    write_new_keystore(output, &encrypt_v2(&pk, new_password)?)
}
fn write_new_keystore(path: &Path, text: &str) -> Result<(), WalletError> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(err)?;
    // Restrict the empty new file before writing; never overwrite an existing key.
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let user = std::env::var("USERNAME").map_err(err)?;
        let domain = std::env::var("USERDOMAIN").map_err(err)?;
        let grant = format!("{domain}\\{user}:(F)");
        let status = std::process::Command::new("icacls")
            .arg(path)
            .args(["/inheritance:r", "/grant:r", &grant])
            .creation_flags(0x08000000)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(err)?;
        if !status.success() {
            return Err(WalletError("无法限制密钥文件权限，未写入密钥".into()));
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(err)?;
    }
    file.write_all(text.as_bytes()).map_err(err)?;
    file.sync_all().map_err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn offline_import_verifies_account_before_encryption() {
        let fixture = hex::encode([1; 32]); // public dummy key, never a real account
        let address = Wallet::from_private_key(&[1; 32]).unwrap().address();
        let mixed_case = format!("0x{}", address[2..].to_uppercase());
        let (actual, encrypted) =
            prepare_keystore(&fixture, "offline-test-password", Some(&mixed_case)).unwrap();
        assert_eq!(actual, address);
        assert_eq!(
            &*decrypt_v2(&encrypted, "offline-test-password").unwrap(),
            &[1; 32]
        );
        assert!(prepare_keystore(
            &fixture,
            "offline-test-password",
            Some("0x0000000000000000000000000000000000000000")
        )
        .is_err());
        assert!(
            prepare_keystore(&fixture, "offline-test-password", Some("not-an-address")).is_err()
        );
        assert!(prepare_keystore(&fixture, "short", Some(&address)).is_err());
    }
    #[test]
    fn v2_kdf_roundtrip_wrong_password_and_tamper() {
        let key = [1u8; 32]; // public test fixture, never a real wallet
        let text = encrypt_v2(&key, "offline-test-password").unwrap();
        assert_eq!(&*decrypt_v2(&text, "offline-test-password").unwrap(), &key);
        assert!(decrypt_v2(&text, "incorrect-password").is_err());
        let mut file: KeyFile = serde_json::from_str(&text).unwrap();
        let mut ciphertext = hex::decode(&file.ciphertext).unwrap();
        ciphertext[0] ^= 1;
        file.ciphertext = hex::encode(ciphertext);
        assert!(decrypt_v2(
            &serde_json::to_string(&file).unwrap(),
            "offline-test-password"
        )
        .is_err());
        assert!(decrypt_v2(&"00".repeat(60), "anything").is_err());
    }
    #[test]
    fn signing_is_canonical_eip155_and_recovers_the_wallet() {
        use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
        let w = Wallet::from_private_key(&[1; 32]).unwrap();
        let tx = Transaction {
            nonce: 4,
            gas_price: 1_000_000_000,
            gas_limit: 200_000,
            to: [2; 20],
            value: 5_000_000_000_000_000,
            data: vec![1, 2, 3, 4],
        };
        let signed = w.sign(&tx).unwrap();
        let bytes = hex::decode(&signed.raw[2..]).unwrap();
        assert_eq!(signed.hash, format!("0x{}", hex::encode(keccak256(&bytes))));
        let rlp = rlp::Rlp::new(&bytes);
        assert_eq!(rlp.item_count().unwrap(), 9);
        let v: u64 = rlp.val_at(6).unwrap();
        let mut r = [0; 32];
        let mut s = [0; 32];
        for (i, out) in [(7, &mut r), (8, &mut s)] {
            let data = rlp.at(i).unwrap().data().unwrap();
            assert!(!data.is_empty() && data[0] != 0 && data.len() <= 32);
            out[32 - data.len()..].copy_from_slice(data);
        }
        let mut unsigned = RlpStream::new_list(9);
        append_core(&mut unsigned, &tx);
        unsigned.append(&56u64).append(&0u8).append(&0u8);
        let recovered = VerifyingKey::recover_from_prehash(
            &keccak256(&unsigned.out()),
            &Signature::from_scalars(r, s).unwrap(),
            RecoveryId::from_byte((v - 147) as u8).unwrap(),
        )
        .unwrap();
        assert_eq!(&recovered, w.signing_key.verifying_key());
    }
}
