use anyhow::Context;
use base64::Engine;
use rsa::{
    pkcs1v15::SigningKey,
    pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePublicKey},
    sha2::{Digest, Sha256},
    signature::{SignatureEncoding, Signer},
    RsaPrivateKey, RsaPublicKey,
};

use crate::{PrivateKey, PublicKey, Result};

pub fn create_token(
    public_key: PublicKey,
    private_key: PrivateKey,
    account_identifier: &str,
    user: &str,
) -> Result<String> {
    let pub_key = RsaPublicKey::from_public_key_pem(public_key.0.as_str()).context("public key")?;
    let priv_key = RsaPrivateKey::from_pkcs8_pem(private_key.0.as_str()).context("private key")?;

    let mut hasher = Sha256::new();
    hasher.update(pub_key.to_public_key_der().context("public key hash")?);
    let hash_bs = hasher.finalize();

    let thumbprint = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&hash_bs[..]);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .context("getting system time")?;

    println!("thumbprint: {thumbprint}");

    let payload = Payload {
        iss: format!("{account_identifier}.{user}.SHA256:{thumbprint}"),
        sub: format!("{account_identifier}.{user}"),
        iat: now.as_millis(),
        // 10 years expiry.
        exp: (now + std::time::Duration::from_secs(10 * 365 * 24 * 60)).as_millis(),
    };

    // serialize payload to json
    let payload_string = serde_json::to_string(&payload).context("serializing payload")?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload_string);
    let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(HEADER);

    let mut hasher = Sha256::new();
    hasher.update(&header_b64);
    hasher.update(b".");
    hasher.update(&payload_b64);
    let digest = hasher.finalize();
    let signing_key = SigningKey::<Sha256>::new(priv_key);
    // Sign the hash with private key

    let hash = signing_key.sign(&digest);

    let b64_hash =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash.to_bytes().as_ref());

    Ok(format!("{}.{}.{}", header_b64, payload_b64, b64_hash))
}

static HEADER: &str = r#"{"alg":"RS256","typ":"JWT"}"#;

#[derive(serde::Serialize)]
struct Payload {
    iss: String,
    sub: String,
    iat: u128,
    exp: u128,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn assert_token_is_correct() {
        let private_key = std::env::var("PRIVATE_KEY_PEM")
            .context("PRIVATE_KEY_PEM env var")
            .expect("PRIVATE_KEY_PEM must be set");
        let public_key = std::env::var("PUBLIC_KEY_PEM")
            .context("PUBLIC_KEY_PEM env var")
            .expect("PUBLIC_KEY_PEM must be set");

        let host = "fx81169.eu-central-1";
        let account_identifier = "fx81169";
        let user = "ParkandoReader";

        let token = create_token(
            PublicKey(public_key),
            PrivateKey(private_key),
            account_identifier,
            user,
        )
        .expect("creating token");

        assert_eq!("asdsad", token);
    }
}
