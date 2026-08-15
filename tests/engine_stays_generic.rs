//! Gate 3 of `PIPELINE_PLAN.md` §9 step 1: **the engine never learns an IR.**
//!
//! §6:558-568 states the rule and names its enforcement: "the proof is
//! mechanical, in the tests: `libpipeline`'s test suite is generic
//! implementations over STAND-IN expression types, never the real IRs. If the
//! engine's tests cannot be written without importing `libtsx` or a Highbay
//! crate, the engine has learned something it must not know - and unlike the
//! composition-boundary regressions this repo has already suffered, this one is
//! caught by a dependency list, not by review."
//!
//! So this file reads the dependency list. It is a check on `Cargo.toml`
//! because that is the artifact the rule is about; a check on the source would
//! be looking for the symptom.
//!
//! **Why a DIRECT check and not a transitive one.** A transitive check would be
//! wrong from §9's step 6 onward: `libpipelinedata` acquires a `libtsx`
//! dependency there (§6b:698-706 - `PipelineExpr::toDAG()` references dag
//! types, and `libtsx` separates its types from its parser behind a feature so
//! that edge pulls no oxc), which makes `libtsx` transitively present in every
//! build of this crate. That is not a violation; the violation would be this
//! crate NAMING it. Rust is what makes the distinction hold: a crate reachable
//! only transitively cannot be named in a `use`, so an engine that wanted to
//! match on a real IR would have to add the edge here, where this test sees it.
//!
//! **What it cannot catch.** A stand-in expression type in a test that is
//! secretly shaped to one real IR's variants would pass. Nothing mechanical
//! reaches that, and pretending otherwise by adding a judgment-shaped check
//! would trade a check that always means something for one that usually does
//! not.

/// Crate names an engine that has stayed generic will never name.
///
/// `libtsx` is where `BindingExpr` lives and where the dag types live; the
/// `highbay_` family and the four below are Highbay's own vocabulary -
/// `NodeGraph`, `Element`, `.hbdef`, `ProgramPlan` and the saga steps
/// (`PIPELINE_PLAN.md`:569-573).
const NEVER: &[&str] = &[
    "libtsx",
    "libhbui",
    "libhbuisim",
    "libteststand",
    "hb_pack",
];

/// The engine's whole permitted dependency surface is the rest of the stack.
/// Named here only to keep the scanner honest - see the assertion.
const THE_REST_OF_THE_STACK: &[&str] = &["libeffects", "libpipelinedata"];

#[test]
fn the_engine_names_no_ir_bearing_crate() {
    let manifest = include_str!("../Cargo.toml");
    let names = dependency_names(manifest);

    // A scanner that quietly found nothing would let this test pass forever.
    for expected in THE_REST_OF_THE_STACK {
        assert!(
            names.iter().any(|n| n == expected),
            "the manifest scanner did not find {expected}, so it is not reading \
             the dependency tables and the checks below prove nothing: {names:?}",
        );
    }

    for name in &names {
        assert!(
            !NEVER.contains(&name.as_str()),
            "libpipeline took a direct dependency on `{name}`. The engine is \
             generic over every expression type and may never name one \
             (PIPELINE_PLAN.md:558-568); whatever needed this belongs behind a \
             libpipelinedata trait, on the stage's side of the seam.",
        );
        assert!(
            !name.starts_with("highbay"),
            "libpipeline took a direct dependency on the Highbay crate \
             `{name}`. See PIPELINE_PLAN.md:558-573: Highbay's app vocabulary \
             stays out of all three stack crates.",
        );
    }
}

#[test]
fn the_engine_reaches_into_no_first_party_crate_by_path() {
    // The name checks above are a denylist, and a denylist does not know about
    // a Highbay crate named something new. This one does not need to: any path
    // dependency pointing into `crates/` is reaching into the Core Repo, which
    // is the one place Highbay's own vocabulary lives (CLAUDE.md's workspace
    // philosophy - crates/ is code strictly tied to the framework, deps/ holds
    // the cleanly abstractable units).
    let manifest = include_str!("../Cargo.toml");
    for path in path_dependency_values(manifest) {
        assert!(
            !path.contains("crates/"),
            "libpipeline has a path dependency into the Core Repo (`{path}`). \
             A stack crate depends only on other stack crates; the direction \
             runs the other way (PIPELINE_PLAN.md:610-623).",
        );
    }
}

/// Every dependency named by a manifest, across all dependency tables
/// (`[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`, their
/// `[target.'..'.*]` forms, and the `[dependencies.name]` spelling).
///
/// Hand-rolled rather than pulling a TOML parser in as a dev-dependency,
/// because a gate whose subject is this crate's dependency list should not add
/// an entry to it in order to run.
fn dependency_names(manifest: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut scanning_keys = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let header = header.trim_start_matches("target.");
            let tail = header.rsplit('.').next().unwrap_or(header);
            if matches!(
                tail,
                "dependencies" | "dev-dependencies" | "build-dependencies"
            ) {
                scanning_keys = true;
            } else {
                // `[dependencies.serde]` - the dep is in the header itself, and
                // its body holds option keys, not dependency names.
                scanning_keys = false;
                let mut parts = header.rsplitn(2, '.');
                let last = parts.next().unwrap_or_default();
                let rest = parts.next().unwrap_or_default();
                if matches!(
                    rest.rsplit('.').next().unwrap_or(rest),
                    "dependencies" | "dev-dependencies" | "build-dependencies"
                ) {
                    names.push(last.to_string());
                }
            }
            continue;
        }
        if !scanning_keys {
            continue;
        }
        if let Some((key, _)) = line.split_once('=') {
            // `name.workspace = true` is a dotted key on `name`.
            let key = key.trim().split('.').next().unwrap_or_default().trim();
            if !key.is_empty() {
                names.push(key.to_string());
            }
        }
    }
    names
}

/// Every `path = "..."` value in a manifest, wherever it appears.
///
/// Deliberately not scoped to the dependency tables: a path that reaches into
/// `crates/` is worth refusing whichever key it hangs off.
fn path_dependency_values(manifest: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let mut rest = line;
        while let Some(at) = rest.find("path") {
            rest = &rest[at + "path".len()..];
            let Some(eq) = rest.find('=') else { break };
            let after = rest[eq + 1..].trim_start();
            let Some(stripped) = after.strip_prefix('"') else {
                continue;
            };
            let Some(end) = stripped.find('"') else { break };
            paths.push(stripped[..end].to_string());
            rest = &stripped[end..];
        }
    }
    paths
}

/// A manifest with every spelling of the edge this file exists to refuse.
///
/// A gate that has only ever seen a passing input is a gate nobody has watched
/// fail. Rather than reason about whether the scanner would catch these, the
/// tests below hand it each one - so the two real checks above are known to be
/// checks, not two ways of reading an empty list.
const A_MANIFEST_THAT_MUST_NOT_PASS: &str = r#"
[package]
name = "libpipeline"
# libtsx = "in a comment, which is not a dependency"

[dependencies]
libeffects = { path = "../libeffects" }
libpipelinedata = { path = "../libpipelinedata" }
libtsx = { path = "../libtsx", default-features = false }

[dev-dependencies]
highbay_data.workspace = true

[target.'cfg(unix)'.dependencies]
libhbui = "0.1"

[dependencies.hb_pack]
path = "../../crates/hb_pack"
features = ["publish"]
"#;

#[test]
fn the_scanner_catches_every_spelling_of_the_edge_it_refuses() {
    let names = dependency_names(A_MANIFEST_THAT_MUST_NOT_PASS);
    for (spelling, expected) in [
        ("a plain [dependencies] key", "libtsx"),
        ("a dotted workspace key in [dev-dependencies]", "highbay_data"),
        ("a target-specific dependency table", "libhbui"),
        ("the [dependencies.name] spelling", "hb_pack"),
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "the scanner missed {spelling} (`{expected}`): {names:?}",
        );
    }
    assert!(
        !names.iter().any(|n| n == "features"),
        "the scanner read option keys out of a [dependencies.name] body as \
         dependency names, which would make it noisy enough to be disabled: {names:?}",
    );
    assert!(
        !names.iter().any(|n| n.contains("in a comment")),
        "the scanner read a comment: {names:?}",
    );

    let paths = path_dependency_values(A_MANIFEST_THAT_MUST_NOT_PASS);
    assert!(
        paths.iter().any(|p| p.contains("crates/")),
        "the path scanner missed the reach into the Core Repo: {paths:?}",
    );
}

#[test]
fn the_path_scanner_sees_the_stacks_own_edges() {
    // Same liveness argument as above: a scanner that finds nothing proves
    // nothing.
    let paths = path_dependency_values(include_str!("../Cargo.toml"));
    assert!(
        paths.iter().any(|p| p.ends_with("libpipelinedata")),
        "the path scanner found no ../libpipelinedata: {paths:?}",
    );
}
