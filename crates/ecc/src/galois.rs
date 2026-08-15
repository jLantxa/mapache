#![forbid(unsafe_code)]

//! GF(2^8) field arithmetic with reduction polynomial 0x11D (x^8 + x^4 + x^3 + x^2 + 1).
//!
//! Lookup-table based implementation. Tables are generated at compile time
//! (`const fn`) without any runtime overhead, atomic flags, or `unsafe` code.
//! Primitive element is 0x02.

/// Precomputed exp and log lookup tables for GF(2^8) with polynomial 0x11D.
const fn generate_tables() -> ([u8; 512], [u8; 256]) {
    let mut exp = [0u8; 512];
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    let mut i = 0;
    while i < 255 {
        exp[i] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= 0x11D;
        }
        i += 1;
    }
    while i < 512 {
        exp[i] = exp[i - 255];
        i += 1;
    }
    (exp, log)
}

const TABLES: ([u8; 512], [u8; 256]) = generate_tables();
pub const EXP: [u8; 512] = TABLES.0;
pub const LOG: [u8; 256] = TABLES.1;

/// Galois field GF(2^8) arithmetic.
pub struct Galois;

impl Galois {
    /// Multiply two elements in GF(2^8).
    #[inline]
    pub const fn mul(a: u8, b: u8) -> u8 {
        if a == 0 || b == 0 {
            0
        } else {
            EXP[LOG[a as usize] as usize + LOG[b as usize] as usize]
        }
    }

    /// Exponentiation: alpha^power in GF(2^8) where alpha = 0x02.
    #[inline]
    pub const fn exp(power: u32) -> u8 {
        if power == 0 {
            1
        } else {
            EXP[(power % 255) as usize]
        }
    }

    /// Multiplicative inverse of `a` in GF(2^8): `a * inv(a) == 1`.
    ///
    /// Returns 0 if `a == 0`.
    #[inline]
    pub const fn inv(a: u8) -> u8 {
        if a == 0 {
            0
        } else {
            EXP[255 - LOG[a as usize] as usize]
        }
    }

    /// Multiply a byte slice by a scalar in GF(2^8), XORing the result into `dst`.
    #[inline]
    pub fn mul_add_scalar(dst: &mut [u8], scalar: u8, src: &[u8]) {
        if scalar == 0 {
            return;
        }
        if scalar == 1 {
            for (d, s) in dst.iter_mut().zip(src.iter()) {
                *d ^= *s;
            }
            return;
        }

        let log_s = LOG[scalar as usize] as usize;
        let mut table = [0u8; 256];
        let mut i = 1;
        while i < 256 {
            table[i] = EXP[log_s + LOG[i] as usize];
            i += 1;
        }

        for (d, s) in dst.iter_mut().zip(src.iter()) {
            *d ^= table[*s as usize];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mul_identity() {
        for a in 0..=255u8 {
            assert_eq!(Galois::mul(a, 1), a);
        }
    }

    #[test]
    fn mul_zero() {
        for a in 0..=255u8 {
            assert_eq!(Galois::mul(a, 0), 0);
            assert_eq!(Galois::mul(0, a), 0);
        }
    }

    #[test]
    fn mul_commutative() {
        for a in 0..=255u8 {
            for b in 0..=255u8 {
                assert_eq!(Galois::mul(a, b), Galois::mul(b, a));
            }
        }
    }

    #[test]
    fn mul_inverse() {
        for a in 1..=255u8 {
            let inv = Galois::inv(a);
            assert_ne!(inv, 0);
            assert_eq!(Galois::mul(a, inv), 1, "inv failed for {a}");
        }
        assert_eq!(Galois::inv(0), 0);
    }

    #[test]
    fn exp_tables_consistent() {
        assert_eq!(Galois::exp(0), 1);
        assert_eq!(Galois::exp(1), 0x02);
        assert_eq!(Galois::exp(255), 1);
    }

    #[test]
    fn mul_add_scalar_test() {
        let mut dst = vec![0u8; 16];
        let src = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        Galois::mul_add_scalar(&mut dst, 2, &src);
        for (d, s) in dst.iter().zip(src.iter()) {
            assert_eq!(*d, Galois::mul(2, *s));
        }
    }
}
