use rand::RngExt;
use std::ops::RangeInclusive;

const TAB: &str = "    ";

fn main() {
    // Generate Gear tables
    let gear = generate_gear_table();
    let gear_ls = gear_table_ls(&gear);

    // Generate masks with 1 to 26 ones
    const MASK_ONE_RANGE: RangeInclusive<u8> = 1..=26;
    let masks: Vec<u64> = MASK_ONE_RANGE.map(generate_mask).collect();
    masks
        .iter()
        .enumerate()
        .for_each(|(i, m)| assert!(m.count_ones() == (i as u32 + 1)));

    print_gear("GEAR", &gear);
    print_gear("GEAR_LS", &gear_ls);
    print_masks("MASKS", &masks);
}

/// Calculate the hash value of one byte value.
fn hash(byte: u8) -> u64 {
    let mut hasher = blake3::Hasher::new();

    const SALT: &[u8] = b"mapache";
    hasher.update(SALT);
    hasher.update(&[byte]);

    let hash_output = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash_output.as_bytes()[0..8]);

    u64::from_le_bytes(bytes)
}

/// Generate the Gear table.
fn generate_gear_table() -> [u64; 256] {
    let gear: [u64; 256] = std::array::from_fn(|i| hash(i as u8));
    gear
}

/// Generate the Gear table with values shifted 1 bit to the left.
fn gear_table_ls(gear: &[u64; 256]) -> [u64; 256] {
    let mut gear_ls = *gear;
    gear_ls.iter_mut().for_each(|byte| *byte <<= 1);
    gear_ls
}

/// Print a table with hex formatting ready to copy to code.
fn print_gear(name: &str, gear: &[u64; 256]) {
    println!("#[rustfmt::skip]\nconst {}: [u64; 256] = [", name);
    for (i, val) in gear.iter().enumerate() {
        if i % 4 == 0 {
            print!("{TAB}");
        }
        print!("0x{:016x}, ", val);
        if i % 4 == 3 {
            println!();
        }
    }
    println!("];\n");
}

fn generate_mask(num_ones: u8) -> u64 {
    assert!(
        num_ones > 0 && num_ones <= 48,
        "num_ones must be between 1 and 48."
    );

    const ACTIVE_BIT_RANGE: std::ops::Range<usize> = 0..48;
    let mut mask: u64 = 0;
    let mut chosen_bits: Vec<u8> = Vec::new();

    while chosen_bits.len() < num_ones as usize {
        let bit_index = rand::rng().random_range(ACTIVE_BIT_RANGE);
        if !chosen_bits.contains(&(bit_index as u8)) {
            chosen_bits.push(bit_index as u8);
        }
    }

    for bit_index in chosen_bits {
        mask |= 1 << bit_index;
    }

    mask
}

fn print_masks(name: &str, masks: &[u64]) {
    println!("const {}: [u64; {}] = [", name, masks.len());
    for mask in masks {
        println!("{TAB}0x{:016x}, ", mask);
    }
    println!("];\n");
}
