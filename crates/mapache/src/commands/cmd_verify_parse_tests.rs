//! Unit tests for `cmd_verify` clap parsers (kept out of cmd_verify.rs for size).

use clap::Parser;

use super::cmd_verify::CmdArgs;

#[derive(Parser, Debug)]
#[command(no_binary_name = true)]
struct VerifyArgsParse {
    #[command(flatten)]
    args: CmdArgs,
}

#[test]
fn parallel_rejects_zero() {
    let err = VerifyArgsParse::try_parse_from(["--read-packs", "--parallel", "0"])
        .expect_err("--parallel 0 must be rejected");
    assert!(
        err.to_string().contains("greater than 0"),
        "unexpected error message: {err}"
    );
}

#[test]
fn parallel_accepts_positive() {
    let parsed = VerifyArgsParse::try_parse_from(["--read-packs", "--parallel", "8"])
        .expect("--parallel 8 must parse");
    assert_eq!(parsed.args.parallel, 8);
}
