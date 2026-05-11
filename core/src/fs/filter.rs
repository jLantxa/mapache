use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, bail};
use futures::StreamExt;

use crate::utils::collections::FxHashMap;

use crate::{
    fs::{self, tree::SerializedNodeStream},
    mapache::ID,
    repository::repo::Repository,
};

#[derive(Default, Debug, Clone)]
struct TrieNode {
    /// Maps path components (e.g., "src") to the index of the child node.
    children: FxHashMap<OsString, usize>,
    /// Indicates if this exact path is marked as a terminal (end of a rule).
    terminal: bool,
    /// True if this node or any of its descendants are terminal.
    subtree_has_terminal: bool,
}

/// A specialized Trie for file paths, optimized for prefix matching.
#[derive(Debug, Default, Clone)]
struct PathTrie {
    nodes: Vec<TrieNode>,
}

impl PathTrie {
    /// Initialize with an estimated capacity to minimize reallocations
    fn with_capacity(cap: usize) -> Self {
        let mut t = Self {
            nodes: Vec::with_capacity(cap),
        };
        t.nodes.push(TrieNode::default()); // Root
        t
    }

    fn insert(&mut self, path: &Path) {
        let mut cur = 0usize;
        for comp in path.components() {
            let key = comp.as_os_str();

            if let Some(&next) = self.nodes[cur].children.get(key) {
                cur = next;
            } else {
                let next_idx = self.nodes.len();
                self.nodes.push(TrieNode::default());
                self.nodes[cur]
                    .children
                    .insert(key.to_os_string(), next_idx);
                cur = next_idx;
            }
        }
        self.nodes[cur].terminal = true;
    }

    fn finalize(&mut self) {
        if self.nodes.is_empty() {
            return;
        }

        for i in (0..self.nodes.len()).rev() {
            let mut has = self.nodes[i].terminal;
            if !has {
                for &child_idx in self.nodes[i].children.values() {
                    if self.nodes[child_idx].subtree_has_terminal {
                        has = true;
                        break;
                    }
                }
            }
            self.nodes[i].subtree_has_terminal = has;
        }
    }

    #[inline]
    fn contains_prefix_of(&self, path: &Path) -> bool {
        let mut cur = 0usize;
        if self.nodes[cur].terminal {
            return true;
        }

        for comp in path.components() {
            match self.nodes[cur].children.get(comp.as_os_str()) {
                Some(&next) => {
                    cur = next;
                    if self.nodes[cur].terminal {
                        return true;
                    }
                }
                None => return false,
            }
        }
        false
    }

    #[inline]
    fn matches_include_semantics(&self, path: &Path) -> bool {
        let mut cur = 0;

        if self.nodes[cur].terminal {
            return true;
        }

        for comp in path.components() {
            match self.nodes[cur].children.get(comp.as_os_str()) {
                Some(&next) => cur = next,
                None => return false,
            }
            // If we hit a terminal node, this path is inside an included folder
            if self.nodes[cur].terminal {
                return true;
            }
        }

        // If the path didn't hit a terminal, check if it's an ancestor of one
        self.nodes[cur].subtree_has_terminal
    }
}

#[derive(Debug, Clone)]
enum GlobToken {
    /// Matches zero or more *path components*.
    DoubleStar,
    /// Matches exactly one *path component*.
    Comp(CompMatcher),
}

#[cfg(unix)]
#[inline]
fn os_bytes(s: &OsStr) -> Option<&[u8]> {
    use std::os::unix::ffi::OsStrExt;
    Some(s.as_bytes())
}

#[cfg(not(unix))]
#[inline]
fn os_bytes(s: &OsStr) -> Option<&[u8]> {
    s.to_str().map(|x| x.as_bytes())
}

#[derive(Debug, Clone)]
enum CompMatcher {
    Literal(OsString),
    Any, // "*"
    /// Wildcards within a component: '*' and '?'
    Wildcard(Box<[u8]>),
}

impl CompMatcher {
    #[inline]
    fn matches(&self, comp: &OsStr) -> bool {
        match self {
            CompMatcher::Literal(lit) => comp == lit.as_os_str(),
            CompMatcher::Any => true,
            CompMatcher::Wildcard(pat) => os_bytes(comp)
                .map(|b| wildmatch_bytes(pat, b))
                .unwrap_or(false),
        }
    }

    #[inline]
    fn from_os(pat: &OsStr) -> Self {
        if let Some(b) = os_bytes(pat) {
            if b == b"*" {
                return CompMatcher::Any;
            }
            if b.iter().any(|&c| c == b'*' || c == b'?') {
                return CompMatcher::Wildcard(b.to_vec().into_boxed_slice());
            }
        }
        // Same semantics as before:
        // - Unix: non-UTF8 still went through bytes and could become Wildcard
        // - non-Unix: non-UTF8 can't be inspected, so becomes Literal
        CompMatcher::Literal(pat.to_os_string())
    }
}

#[inline]
fn wildmatch_bytes(pat: &[u8], text: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;

    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == text[ti]) {
            pi += 1;
            ti += 1;
            continue;
        }

        if pi < pat.len() && pat[pi] == b'*' {
            while pi < pat.len() && pat[pi] == b'*' {
                pi += 1;
            }
            star_pi = Some(pi);
            star_ti = ti;
            continue;
        }

        if let Some(sp) = star_pi {
            star_ti += 1;
            ti = star_ti;
            pi = sp;
            continue;
        }

        return false;
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }
    pi == pat.len()
}

#[derive(Debug, Clone)]
pub(crate) struct GlobRule {
    tokens: Box<[GlobToken]>,
    ds_mask: u128, // valid when tokens.len() <= 127
    len: u8,       // tokens.len()
    accept: u128,  // 1u128 << len (0 if len > 127)
}

impl GlobRule {
    // Accept state index is tokens.len(); needs m+1 bits. u128 => m <= 127.
    const MAX_TOKENS_U128: usize = 127;

    pub(crate) fn new(pattern: &Path) -> Self {
        let mut tokens = Vec::new();
        for comp in pattern.components() {
            tokens.push(Self::compile_component(comp.as_os_str()));
        }

        let len_usize = tokens.len();
        let len = len_usize as u8;

        let mut ds_mask = 0u128;
        let accept = if len_usize <= Self::MAX_TOKENS_U128 {
            for (i, t) in tokens.iter().enumerate() {
                if matches!(t, GlobToken::DoubleStar) {
                    ds_mask |= 1u128 << i;
                }
            }
            1u128 << len_usize
        } else {
            0
        };

        Self {
            tokens: tokens.into_boxed_slice(),
            ds_mask,
            len,
            accept,
        }
    }

    #[inline]
    fn len_usize(&self) -> usize {
        self.len as usize
    }

    fn has_meta(pattern: &Path) -> bool {
        pattern
            .components()
            .any(|c| component_has_glob_meta(c.as_os_str()))
    }

    fn compile_component(os: &OsStr) -> GlobToken {
        if is_double_star_component(os) {
            GlobToken::DoubleStar
        } else {
            GlobToken::Comp(CompMatcher::from_os(os))
        }
    }

    /// Exclude semantics: rule matches if it matches any *prefix* of `path`.
    #[inline]
    pub fn matches_for_exclude(&self, path: &Path) -> bool {
        if self.tokens.len() <= Self::MAX_TOKENS_U128 {
            self.matches_prefix_u128(path)
        } else {
            self.matches_prefix_fallback(path)
        }
    }

    /// Returns true ONLY if the entire path matches the glob pattern.
    pub fn is_strict_match(&self, path: &Path) -> bool {
        let mut states: u128 = 1; // Start state
        states = self.epsilon_closure_u128(states);

        for comp in path.components() {
            states = self.step_u128(states, comp.as_os_str());
            if states == 0 {
                return false; // Path diverged from glob
            }
        }

        // Check if the final state is an 'accept' state
        (states & self.accept) != 0
    }

    #[inline]
    fn epsilon_closure_u128(&self, mut s: u128) -> u128 {
        // Propagate through consecutive **.
        // next = s | ((s & ds_mask) << 1) until stable.
        // In practice, runs of ** are short; loop is fine and usually 1-2 iterations.
        loop {
            let next = s | ((s & self.ds_mask) << 1);
            if next == s {
                return s;
            }
            s = next;
        }
    }

    #[inline]
    fn step_u128(&self, states: u128, comp: &OsStr) -> u128 {
        let mut next: u128 = 0;
        let mut s = states;
        let m = self.len_usize();

        while s != 0 {
            let i = s.trailing_zeros() as usize;
            s &= s - 1;

            if i >= m {
                continue; // accept state has no outgoing transitions
            }

            match &self.tokens[i] {
                GlobToken::DoubleStar => next |= 1u128 << i,
                GlobToken::Comp(mm) => {
                    if mm.matches(comp) {
                        next |= 1u128 << (i + 1);
                    }
                }
            }
        }

        self.epsilon_closure_u128(next)
    }

    fn matches_prefix_u128(&self, path: &Path) -> bool {
        let accept = self.accept;

        let mut states: u128 = 1; // state 0
        states = self.epsilon_closure_u128(states);
        if (states & accept) != 0 {
            return true;
        }

        for comp in path.components() {
            states = self.step_u128(states, comp.as_os_str());
            if states == 0 {
                return false;
            }
            if (states & accept) != 0 {
                return true;
            }
        }

        false
    }

    // Rare fallback for very deep patterns (>127 components): use a greedy backtracking matcher.
    fn collect_path_components(path: &Path) -> Vec<&OsStr> {
        path.components().map(|c| c.as_os_str()).collect()
    }

    #[cold]
    fn matches_prefix_fallback(&self, path: &Path) -> bool {
        let comps = Self::collect_path_components(path);
        match_prefix_components(&self.tokens, &comps)
    }
}

#[inline]
fn component_has_glob_meta(os: &OsStr) -> bool {
    match os_bytes(os) {
        Some(b) => {
            // order matters a tiny bit: "**" is a very common check
            b == b"**" || b.iter().any(|&c| c == b'*' || c == b'?')
        }
        None => false,
    }
}

#[inline]
fn is_double_star_component(os: &OsStr) -> bool {
    os_bytes(os).is_some_and(|b| b == b"**")
}

// ----- Fallback matchers (only used when pattern is extremely deep) -----

fn match_prefix_components(pattern: &[GlobToken], path: &[&OsStr]) -> bool {
    // Does `pattern` match a prefix of `path`? (accept as soon as pattern is consumed)
    let mut p = 0usize;
    let mut t = 0usize;

    let mut star: Option<(usize, usize)> = None; // (pattern_after_star, path_index)

    // Skip leading **
    while p < pattern.len() && matches!(pattern[p], GlobToken::DoubleStar) {
        star = Some((p + 1, t));
        p += 1;
        if p == pattern.len() {
            return true;
        }
    }

    while t < path.len() {
        if p == pattern.len() {
            return true;
        }

        match &pattern[p] {
            GlobToken::DoubleStar => {
                star = Some((p + 1, t));
                p += 1;
                if p == pattern.len() {
                    return true;
                }
            }
            GlobToken::Comp(m) => {
                if m.matches(path[t]) {
                    p += 1;
                    t += 1;
                    if p == pattern.len() {
                        return true;
                    }
                } else if let Some((p_after, t_star)) = star {
                    // Extend the last **
                    let t_next = t_star + 1;
                    if t_next > path.len() {
                        return false;
                    }
                    star = Some((p_after, t_next));
                    t = t_next;
                    p = p_after;
                } else {
                    return false;
                }
            }
        }
    }

    // Path ended; pattern matches prefix only if it can finish by consuming empty (** only)
    while p < pattern.len() && matches!(pattern[p], GlobToken::DoubleStar) {
        p += 1;
    }
    p == pattern.len()
}

#[derive(Debug, Clone)]
pub struct PathFilter {
    include_trie: Option<PathTrie>,
    exclude_trie: Option<PathTrie>,
    exclude_globs: Box<[GlobRule]>,
}

impl PathFilter {
    pub fn new(include: Option<Vec<PathBuf>>, exclude: Option<Vec<PathBuf>>) -> Self {
        let include_trie = include.map(|paths| {
            let mut t = PathTrie::with_capacity(paths.len().max(1));
            for p in paths {
                t.insert(&p);
            }
            t.finalize();
            t
        });

        let (exclude_trie, exclude_globs) = exclude
            .map(|paths| {
                let mut estimated_nodes = 1usize;
                let mut globs = Vec::new();
                let mut literals = Vec::new();

                for p in paths {
                    if GlobRule::has_meta(&p) {
                        globs.push(GlobRule::new(&p));
                    } else {
                        estimated_nodes += p.components().count();
                        literals.push(p);
                    }
                }

                let trie = if !literals.is_empty() {
                    let mut t = PathTrie::with_capacity(estimated_nodes);
                    for p in literals {
                        t.insert(&p);
                    }
                    t.finalize();
                    Some(t)
                } else {
                    None
                };

                (trie, globs.into_boxed_slice())
            })
            .unwrap_or((None, Box::new([])));

        Self {
            include_trie,
            exclude_trie,
            exclude_globs,
        }
    }

    #[inline]
    pub fn allow(&self, path: &Path) -> bool {
        if let Some(trie) = &self.exclude_trie
            && trie.contains_prefix_of(path)
        {
            return false;
        }
        for g in &self.exclude_globs {
            if g.matches_for_exclude(path) {
                return false;
            }
        }

        let Some(inc) = &self.include_trie else {
            return true;
        };

        inc.matches_include_semantics(path)
    }
}

pub(crate) fn is_glob(path_str: &str) -> bool {
    path_str.contains('*') || path_str.contains('?')
}

// Normalize the exclude paths
pub(crate) fn normalized_exclude_paths(
    epaths: Option<&Vec<String>>,
) -> Result<Option<Vec<PathBuf>>> {
    if let Some(path_strs) = epaths {
        let mut normalized_vec = Vec::new();
        for path_str in path_strs {
            let pbuf = PathBuf::from(path_str);

            if is_glob(path_str) {
                normalized_vec.push(pbuf);
            } else {
                match fs::get_absolute_normalized_path(&pbuf) {
                    Ok(normalized_path) => normalized_vec.push(normalized_path),
                    Err(e) => bail!("{path_str:?}: {e}"),
                };
            }
        }
        Ok(Some(normalized_vec))
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_relative_filter_paths(iepaths: Option<&Vec<String>>) -> Option<Vec<PathBuf>> {
    iepaths.map(|paths| paths.iter().map(PathBuf::from).collect())
}

pub async fn expand_include_paths(
    repo: Arc<Repository>,
    tree_id: &ID,
    includes: Option<&[String]>,
    excludes: Option<Vec<PathBuf>>,
) -> Result<Option<Vec<PathBuf>>> {
    let Some(includes) = includes else {
        return Ok(None);
    };

    let mut fixed_includes = Vec::new();
    let mut include_rules = Vec::new();

    for path in includes {
        if is_glob(path) {
            include_rules.push(GlobRule::new(Path::new(path)));
        } else {
            fixed_includes.push(PathBuf::from(path));
        }
    }

    if !include_rules.is_empty() {
        let mut stream =
            SerializedNodeStream::new(repo, Some(*tree_id), PathBuf::new(), None, excludes).await?;

        while let Some(res) = stream.next().await {
            let (path, stream_node_res) = res?;
            let stream_node = stream_node_res?;
            if stream_node.node.is_file() && include_rules.iter().any(|g| g.is_strict_match(&path))
            {
                fixed_includes.push(path);
            }
        }
    }

    fixed_includes.sort();
    fixed_includes.dedup();

    Ok(Some(fixed_includes))
}

pub(crate) fn read_filtered_paths_from_file(path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect())
}

pub(crate) fn merge_filtered_paths(
    a: Option<&Vec<String>>,
    b: Option<&Vec<String>>,
) -> Option<Vec<String>> {
    if a.is_none() && b.is_none() {
        return None;
    }

    let mut unique_paths = HashSet::new();

    if let Some(a_paths) = a {
        unique_paths.extend(a_paths.iter().cloned());
    }
    if let Some(b_paths) = b {
        unique_paths.extend(b_paths.iter().cloned());
    }

    Some(unique_paths.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_path_filter_existing_behavior() {
        let p_abc = Path::new("/a/b/c");
        let p_xyz = Path::new("/x/y/z");
        let p_ab = Path::new("/a/b");
        let p_abcd = Path::new("/a/b/c/d");
        let p_img = Path::new("/a/b/photo.png");

        // --- No filter (Default Allow) ---
        let filter = PathFilter::new(None, None);
        assert!(filter.allow(p_abc));

        // --- Exclude Only ---
        let filter = PathFilter::new(None, Some(vec![PathBuf::from("/a")]));
        assert!(!filter.allow(p_abc));
        assert!(filter.allow(p_xyz));
        assert!(!filter.allow(p_img));

        // --- Include Only ---
        let filter = PathFilter::new(Some(vec![PathBuf::from("/a/b/c")]), None);
        assert!(filter.allow(p_abc));
        assert!(!filter.allow(p_xyz));
        assert!(filter.allow(p_ab));
        assert!(filter.allow(p_abcd));

        // --- Exclude vs Include Priority ---
        let filter = PathFilter::new(
            Some(vec![PathBuf::from("/a")]),
            Some(vec![PathBuf::from("/a/b")]),
        );
        assert!(!filter.allow(p_abc));
        assert!(!filter.allow(p_ab));
        assert!(filter.allow(Path::new("/a/other.txt")));
    }

    #[test]
    fn test_glob_exclude_prefix_semantics() {
        // Exclude: /a/*/c (and descendants)
        let filter = PathFilter::new(None, Some(vec![PathBuf::from("/a/*/c")]));

        assert!(filter.allow(Path::new("/a")));
        assert!(filter.allow(Path::new("/a/b")));
        assert!(!filter.allow(Path::new("/a/b/c")));
        assert!(!filter.allow(Path::new("/a/b/c/d")));
        assert!(filter.allow(Path::new("/a/b/d")));
    }

    #[test]
    fn test_glob_exclude_overrides_glob_include() {
        let filter = PathFilter::new(None, Some(vec![PathBuf::from("/a/*/c")]));

        assert!(filter.allow(Path::new("/a/b")));
        assert!(!filter.allow(Path::new("/a/b/c")));
        assert!(!filter.allow(Path::new("/a/b/c/d")));
        assert!(filter.allow(Path::new("/a/b/d")));
    }

    #[test]
    fn test_wildmatch_bytes() {
        assert!(wildmatch_bytes(b"a*b", b"ab"));
        assert!(wildmatch_bytes(b"a*b", b"axb"));
        assert!(wildmatch_bytes(b"a*b", b"axxxb"));
        assert!(!wildmatch_bytes(b"a*b", b"ax"));
        assert!(wildmatch_bytes(b"a?b", b"axb"));
        assert!(!wildmatch_bytes(b"a?b", b"ab"));
        assert!(wildmatch_bytes(b"*.txt", b"foo.txt"));
        assert!(!wildmatch_bytes(b"*.txt", b"foo.png"));
        assert!(wildmatch_bytes(b"**", b"anything"));
        assert!(wildmatch_bytes(b"**a", b"ba"));
        assert!(wildmatch_bytes(b"**a", b"a"));
        assert!(wildmatch_bytes(b"a**b", b"ab"));
        assert!(wildmatch_bytes(b"a**b", b"axb"));
        assert!(wildmatch_bytes(b"*?*a", b"ba"));
        assert!(wildmatch_bytes(b"*?*a", b"bba"));
        assert!(!wildmatch_bytes(b"*?*a", b"a"));
        assert!(wildmatch_bytes(b"", b""));
        assert!(!wildmatch_bytes(b"a", b""));
        assert!(!wildmatch_bytes(b"", b"a"));
    }

    #[test]
    fn test_glob_rule_strict_match() {
        let rule = GlobRule::new(Path::new("src/**/*.rs"));
        assert!(rule.is_strict_match(Path::new("src/main.rs")));
        assert!(rule.is_strict_match(Path::new("src/utils/mod.rs")));
        assert!(rule.is_strict_match(Path::new("src/a/b/c/d.rs")));
        assert!(!rule.is_strict_match(Path::new("src/main.c")));
        assert!(!rule.is_strict_match(Path::new("tests/test.rs")));

        let rule2 = GlobRule::new(Path::new("a/*/c"));
        assert!(rule2.is_strict_match(Path::new("a/b/c")));
        assert!(!rule2.is_strict_match(Path::new("a/c")));
        assert!(!rule2.is_strict_match(Path::new("a/b/d/c")));

        let rule3 = GlobRule::new(Path::new("**/target/*.o"));
        assert!(rule3.is_strict_match(Path::new("target/main.o")));
        assert!(rule3.is_strict_match(Path::new("a/b/target/test.o")));
        assert!(!rule3.is_strict_match(Path::new("a/target/b/test.o")));
    }

    #[test]
    fn test_glob_rule_prefix_match_exclude() {
        let rule = GlobRule::new(Path::new("a/b/c"));
        assert!(rule.matches_for_exclude(Path::new("a/b/c")));
        assert!(rule.matches_for_exclude(Path::new("a/b/c/d")));
        assert!(!rule.matches_for_exclude(Path::new("a/b")));

        let rule2 = GlobRule::new(Path::new("a/*/c"));
        assert!(rule2.matches_for_exclude(Path::new("a/x/c")));
        assert!(rule2.matches_for_exclude(Path::new("a/x/c/d")));
        assert!(!rule2.matches_for_exclude(Path::new("a/x")));
    }

    #[test]
    fn test_path_trie() {
        let mut trie = PathTrie::with_capacity(10);
        trie.insert(Path::new("a/b/c"));
        trie.finalize();

        assert!(trie.contains_prefix_of(Path::new("a/b/c")));
        assert!(trie.contains_prefix_of(Path::new("a/b/c/d")));
        assert!(!trie.contains_prefix_of(Path::new("a/b")));
        assert!(!trie.contains_prefix_of(Path::new("x")));

        // test matches_include_semantics
        assert!(trie.matches_include_semantics(Path::new("a")));
        assert!(trie.matches_include_semantics(Path::new("a/b")));
        assert!(trie.matches_include_semantics(Path::new("a/b/c")));
        assert!(trie.matches_include_semantics(Path::new("a/b/c/d")));
        assert!(!trie.matches_include_semantics(Path::new("x")));
    }
}
