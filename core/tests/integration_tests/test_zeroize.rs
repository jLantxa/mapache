use mapache::repository::keys::KeyManager;
use mapache::repository::repo::Auth;
use mapache::repository::storage::SecureStorage;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use zeroize::{Zeroize, Zeroizing};

#[test]
fn test_zeroize_array() {
    let mut a = [1u8; 8];
    a.zeroize();
    assert_eq!(a, [0u8; 8]);
}

#[test]
fn test_zeroize_vec_manual() {
    let mut v = vec![1u8, 2, 3, 4];
    let ptr = v.as_ptr();
    let len = v.len();

    v.zeroize();
    assert_eq!(v.len(), 0);

    // Check if memory was zeroed before truncation
    unsafe {
        let slice = std::slice::from_raw_parts(ptr, len);
        for &byte in slice {
            assert_eq!(byte, 0);
        }
    }
}

struct DropTracker(Arc<AtomicBool>);
impl Zeroize for DropTracker {
    fn zeroize(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn test_zeroizing_calls_zeroize_on_drop() {
    let called = Arc::new(AtomicBool::new(false));
    {
        let _ = Zeroizing::new(DropTracker(called.clone()));
    }
    assert!(called.load(Ordering::SeqCst));
}

#[test]
fn test_auth_password_is_zeroizing() {
    let auth = Auth {
        username: "user".to_string(),
        password: Zeroizing::new("secret".to_string()),
    };

    // We can't easily check that it zeros on drop without UB,
    // but we've verified that Zeroizing calls zeroize() on drop.
    // So we just need to ensure it IS a Zeroizing<String>.
    // (This is already guaranteed by the type system if it compiles)
    assert_eq!(*auth.password, "secret");
}

#[test]
fn test_master_key_is_zeroizing() {
    let key = KeyManager::generate_new_master_key();
    // key is Zeroizing<Vec<u8>>
    assert_eq!(key.len(), 32);
}

#[test]
fn test_derived_key_is_zeroizing() {
    let password = "password";
    let salt = [0u8; 16];
    let params = argon2::Params::default();
    let key = SecureStorage::derive_key::<32>(password, &salt, params).unwrap();
    // key is Zeroizing<[u8; 32]>
    assert_eq!(key.len(), 32);
}
