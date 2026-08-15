//! Gate 3 of `PIPELINE_PLAN.md` §9 step 1: **the engine never learns an IR.**
//!
//! §6:584-589 states the rule and names its enforcement: "the proof is
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
//! **The check is TRANSITIVE** - the whole stack's tree, not this crate's
//! direct edges (§6:591-604). It was direct in step 1, for a reason that has
//! since expired: `libpipelinedata` was going to acquire a types-only `libtsx`
//! dependency at step 6, which would have put `libtsx` in this crate's
//! transitive list with nothing having gone wrong. `PipelineExpr` now lives in
//! `highbay_data` instead (`52b6562`), so **nothing in the stack depends on
//! `libtsx` at any step** and the stronger check is true for free. The plan's
//! reason for preferring it: "a rule that holds transitively cannot be evaded
//! by routing an edge through a sibling". The direct check would still have
//! been SUFFICIENT - a crate reachable only transitively cannot be named in a
//! `use` - but sufficiency is not a reason to keep the weaker gate.
//!
//! **How the tree is walked without a TOML parser or a `cargo` invocation.**
//! Every crate in the stack is a PATH dependency, so the walk is: read a
//! manifest, collect its dependency names, follow its `path = ".."` values to
//! more manifests. It reads no lock file and shells out to nothing, so it is
//! unaffected by whether the workspace currently resolves - a gate that fails
//! because a sibling crate is mid-edit is noise, and noise gets switched off.
//!
//! **What it cannot catch**, in two parts. A stand-in expression type in a test
//! that is secretly shaped to one real IR's variants would pass; nothing
//! mechanical reaches that, and a judgment-shaped check would trade a gate that
//! always means something for one that usually does not. And the walk follows
//! PATH dependencies only, so it would miss an IR-bearing crate pulled in from
//! a registry or a git URL by a stack crate. That is not the shape the rule is
//! about - `libtsx` and the Highbay crates are unpublished path dependencies of
//! this workspace - and the direct-name checks below still see any such edge in
//! this crate's own manifest.

/// Crate names an engine that has stayed generic will never name.
///
/// `libtsx` is where `BindingExpr` lives and where the dag types live; the
/// `highbay_` family and the four below are Highbay's own vocabulary -
/// `NodeGraph`, `Element`, `.hbdef`, `ProgramPlan` and the saga steps
/// (`PIPELINE_PLAN.md`:605-609).
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
fn the_stack_names_no_ir_bearing_crate_at_any_depth() {
    let walk = walk_the_stack();

    // A scanner that quietly found nothing would let this test pass forever.
    for expected in THE_REST_OF_THE_STACK {
        assert!(
            walk.dependencies.iter().any(|d| d.name == *expected),
            "the manifest scanner did not find {expected}, so it is not reading \
             the dependency tables and the checks below prove nothing: {:?}",
            walk.dependencies,
        );
    }
    assert!(
        walk.dependencies
            .iter()
            .any(|d| d.name == "libeffects" && d.named_by.contains("libpipelinedata")),
        "the walk never followed a path dependency into another manifest, so it \
         is a direct check wearing a transitive check's name: {:?}",
        walk.dependencies,
    );

    for reached in &walk.dependencies {
        let named_by = &reached.named_by;
        assert!(
            !NEVER.contains(&reached.name.as_str()),
            "`{}` reaches `{}` (named by {named_by}). The stack is generic over \
             every expression type and may never name one at any depth \
             (PIPELINE_PLAN.md:584-604); whatever needed this belongs behind a \
             libpipelinedata trait, on the stage's side of the seam.",
            env!("CARGO_PKG_NAME"),
            reached.name,
        );
        assert!(
            !reached.name.starts_with("highbay"),
            "`{}` reaches the Highbay crate `{}` (named by {named_by}). See \
             PIPELINE_PLAN.md:605-609: Highbay's app vocabulary stays out of \
             all three stack crates.",
            env!("CARGO_PKG_NAME"),
            reached.name,
        );
    }
}

#[test]
fn the_stack_reaches_into_no_first_party_crate_by_path() {
    // The name checks above are a denylist, and a denylist does not know about
    // a Highbay crate named something new. This one does not need to: any path
    // dependency pointing into `crates/` is reaching into the Core Repo, which
    // is the one place Highbay's own vocabulary lives (CLAUDE.md's workspace
    // philosophy - crates/ is code strictly tied to the framework, deps/ holds
    // the cleanly abstractable units).
    let walk = walk_the_stack();
    assert!(
        !walk.paths.is_empty(),
        "the path scanner found no path dependencies at all, so it proves nothing",
    );
    for reached in &walk.paths {
        assert!(
            !reached.name.contains("crates/"),
            "a stack crate has a path dependency into the Core Repo (`{}`, \
             named by {}). A stack crate depends only on other stack crates; \
             the direction runs the other way (PIPELINE_PLAN.md:655-664).",
            reached.name,
            reached.named_by,
        );
    }
}

/// Something a manifest named, and which manifest named it.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Reached {
    name: String,
    named_by: String,
}

/// Everything the stack's manifests name, transitively.
#[derive(Debug, Default)]
struct Walk {
    /// Dependency names, from every manifest reached.
    dependencies: Vec<Reached>,
    /// `path = ".."` values, from every manifest reached.
    paths: Vec<Reached>,
    /// Which manifests were read, in visit order. A walk that read one manifest
    /// is a direct check.
    manifests: Vec<String>,
}

/// The real walk, from this crate's manifest outward.
fn walk_the_stack() -> Walk {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    walk_manifests(&root, &|path| std::fs::read_to_string(path).ok())
}

/// Follow path dependencies from `root`, collecting what each manifest names.
///
/// `load` is a parameter rather than `std::fs` inline so the walk can be handed
/// a manifest tree that must not pass - including one where the offending crate
/// is only reachable at depth 2, which is exactly the case a direct check
/// misses.
///
/// Terminates on a cycle: cargo permits a dev-dependency cycle between two
/// crates, so a walk that assumed a tree would hang on a legal workspace.
fn walk_manifests(
    root: &std::path::Path,
    load: &dyn Fn(&std::path::Path) -> Option<String>,
) -> Walk {
    let mut walk = Walk::default();
    let mut visited: Vec<std::path::PathBuf> = Vec::new();
    let mut queue: std::collections::VecDeque<std::path::PathBuf> =
        std::collections::VecDeque::from([normalize(root)]);

    while let Some(manifest) = queue.pop_front() {
        if visited.contains(&manifest) {
            continue;
        }
        visited.push(manifest.clone());
        let Some(text) = load(&manifest) else {
            continue;
        };
        let label = crate_dir_of(&manifest);
        walk.manifests.push(label.clone());
        for name in dependency_names(&text) {
            walk.dependencies.push(Reached {
                name,
                named_by: label.clone(),
            });
        }
        let dir = manifest.parent().unwrap_or(&manifest).to_path_buf();
        for path in path_dependency_values(&text) {
            walk.paths.push(Reached {
                name: path.clone(),
                named_by: label.clone(),
            });
            queue.push_back(normalize(&dir.join(&path).join("Cargo.toml")));
        }
    }
    walk
}

/// `..` resolved lexically, so a fake tree's keys are predictable and the real
/// walk does not visit one crate under two spellings.
fn normalize(path: &std::path::Path) -> std::path::PathBuf {
    let mut out = std::path::PathBuf::new();
    for part in path.components() {
        match part {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

/// The directory a manifest sits in - what a crate is called for a message.
fn crate_dir_of(manifest: &std::path::Path) -> String {
    manifest
        .parent()
        .and_then(|dir| dir.file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| manifest.to_string_lossy().into_owned())
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

/// A stack whose engine manifest is CLEAN and which is still a violation: the
/// `libtsx` edge is two crates away, and a Highbay crate three.
///
/// This is the input that separates the transitive check from the direct one.
/// A direct check reads the first manifest, finds `libeffects` and
/// `libpipelinedata`, and passes.
fn a_stack_that_must_not_pass(path: &std::path::Path) -> Option<String> {
    let manifest = match path.to_string_lossy().as_ref() {
        "/stack/libpipeline/Cargo.toml" => {
            "[dependencies]\n\
             libeffects = { path = \"../libeffects\" }\n\
             libpipelinedata = { path = \"../libpipelinedata\" }\n"
        }
        "/stack/libpipelinedata/Cargo.toml" => {
            "[dependencies]\n\
             libeffects = { path = \"../libeffects\" }\n\
             libtsx = { path = \"../libtsx\", default-features = false }\n\
             libpipelinedata-macros = { path = \"./libpipelinedata-macros\" }\n"
        }
        "/stack/libpipelinedata/libpipelinedata-macros/Cargo.toml" => {
            "[dependencies]\n\
             syn = \"2\"\n\
             highbay_macros = { path = \"../../../crates/highbay_macros\" }\n"
        }
        "/stack/libeffects/Cargo.toml" => "[dependencies]\n",
        _ => return None,
    };
    Some(manifest.to_string())
}

#[test]
fn the_walk_catches_an_ir_bearing_crate_two_manifests_away() {
    let root = std::path::Path::new("/stack/libpipeline/Cargo.toml");
    let walk = walk_manifests(root, &a_stack_that_must_not_pass);

    assert!(
        !dependency_names(&a_stack_that_must_not_pass(root).unwrap())
            .iter()
            .any(|name| name == "libtsx"),
        "the fake root manifest must be clean, or this proves nothing about \
         what the direct check would have missed",
    );
    assert!(
        walk.dependencies
            .iter()
            .any(|d| d.name == "libtsx" && d.named_by == "libpipelinedata"),
        "the walk missed the depth-2 libtsx edge: {:?}",
        walk.dependencies,
    );
    assert!(
        walk.dependencies
            .iter()
            .any(|d| d.name == "highbay_macros" && d.named_by == "libpipelinedata-macros"),
        "the walk missed the depth-3 Highbay edge: {:?}",
        walk.dependencies,
    );
    assert!(
        walk.paths.iter().any(|p| p.name.contains("crates/")),
        "the walk missed the reach into the Core Repo: {:?}",
        walk.paths,
    );
    assert_eq!(
        walk.manifests,
        [
            "libpipeline",
            "libeffects",
            "libpipelinedata",
            "libpipelinedata-macros",
        ],
        "each manifest read once, breadth first",
    );
}

#[test]
fn the_walk_terminates_on_a_dependency_cycle() {
    // Cargo permits a dev-dependency cycle between two crates, so a walk that
    // assumed a tree would hang on a legal workspace rather than fail.
    let cyclic = |path: &std::path::Path| match path.to_string_lossy().as_ref() {
        "/stack/one/Cargo.toml" => {
            Some("[dependencies]\ntwo = { path = \"../two\" }\n".to_string())
        }
        "/stack/two/Cargo.toml" => {
            Some("[dev-dependencies]\none = { path = \"../one\" }\n".to_string())
        }
        _ => None,
    };
    let walk = walk_manifests(std::path::Path::new("/stack/one/Cargo.toml"), &cyclic);
    assert_eq!(walk.manifests, ["one", "two"]);
}

#[test]
fn a_manifest_that_cannot_be_read_is_skipped_rather_than_fatal() {
    // A path dependency whose manifest is missing - a submodule not checked
    // out, a sibling crate mid-rename - must not fail this gate. What it
    // reports on is what it can read, and it says so by continuing.
    let root = std::path::Path::new("/stack/libpipeline/Cargo.toml");
    let only_the_root = |path: &std::path::Path| {
        (path == root).then(|| a_stack_that_must_not_pass(path).unwrap())
    };
    let walk = walk_manifests(root, &only_the_root);
    assert_eq!(walk.manifests, ["libpipeline"]);
    assert_eq!(walk.paths.len(), 2, "the unreadable edges are still recorded");
}

