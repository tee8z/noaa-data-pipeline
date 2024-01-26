use std::{fs::File, io::Read, str::FromStr, sync::Arc};
use kormir::{
    bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey},
    Oracle,
};

use crate::{DbManager, OracleStorage};

#[derive(Clone)]
pub struct OracleService {
    public_key: PublicKey,
    oracle: Arc<Oracle<OracleStorage>>,
}

impl OracleService {
    // TODO (@tee8z): switch to a real error type when failing to get the signing key from a file
    pub fn new(db: DbManager, private_key_path: String) -> Self {
        //signing_key: SecretKey, nonce_xpriv: ExtendedPrivKey
        let mut file = File::open(private_key_path).unwrap();
        // Read the file contents into a string
        let mut raw_private_key = String::new();
        file.read_to_string(&mut raw_private_key).unwrap();

        let signing_key = SecretKey::from_str(&raw_private_key).unwrap();
        let oracle_store = OracleStorage::new(db);
        let oracle = Oracle::from_signing_key(
            oracle_store,
            signing_key,
        ).unwrap();
        let secp = Secp256k1::new();
        let public_key = signing_key.public_key(&secp);

        Self {
            public_key,
            oracle: Arc::new(oracle),
        }
    }
}
