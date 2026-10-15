//! Shared build-time instrumentation. Insertions preserve source lines and normal Rust module loading.
use proc_macro2::Span;
use quote::ToTokens;
use std::{
    fs,
    path::{Path, PathBuf},
};
use syn::visit_mut::VisitMut;
use syn::{
    Expr, ReturnType, Signature, Type,
    spanned::Spanned,
    visit::{self, Visit},
};

pub struct Generated {
    pub entry: PathBuf,
    pub inventory: Vec<(String, String)>,
}
pub fn generate(source: &Path, entry: &str, out: &Path) -> Result<Generated, String> {
    fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let mut files = vec![];
    walk(source, &mut files)?;
    files.sort();
    let mut inventory = vec![];
    println!("cargo:rerun-if-changed={}", source.display());
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        let relative = path.strip_prefix(source).map_err(|e| e.to_string())?;
        let text = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let parsed = syn::parse_file(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        let namespace = if relative == Path::new(entry) {
            vec![]
        } else {
            relative
                .with_extension("")
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .filter(|s| s != "mod")
                .collect()
        };
        let mut scanner = Scanner {
            edits: vec![],
            inventory: vec![],
            namespace,
            depth: 0,
            parent: path.parent().unwrap(),
        };
        scanner.visit_file(&parsed);
        let mut edits = scanner.edits;
        if relative == Path::new(entry) {
            for attr in &parsed.attrs {
                if !attr.path().is_ident("doc") {
                    return Err(
                        "keep crate attributes in the entry wrapper outside include!".into(),
                    );
                }
                // Inner docs cannot appear in include!: retain the line and all following locations.
                let range = attr.span().byte_range();
                let replacement = text[range.clone()].replace("//!", "//");
                edits.push(Edit {
                    start: range.start,
                    end: range.end,
                    text: replacement,
                    depth: 0,
                    close: false,
                });
            }
        }
        edits.sort_by(|a, b| {
            b.start
                .cmp(&a.start)
                .then_with(|| a.close.cmp(&b.close))
                .then_with(|| {
                    if a.close {
                        a.depth.cmp(&b.depth)
                    } else {
                        b.depth.cmp(&a.depth)
                    }
                })
        });
        let mut output = text;
        for edit in edits {
            output.replace_range(edit.start..edit.end, &edit.text);
        }
        if output.lines().count()
            != fs::read_to_string(&path)
                .map_err(|e| e.to_string())?
                .lines()
                .count()
        {
            return Err(format!(
                "instrumentation changed source lines: {}",
                path.display()
            ));
        }
        let target = out.join(relative);
        fs::create_dir_all(target.parent().unwrap()).map_err(|e| e.to_string())?;
        fs::write(target, output).map_err(|e| e.to_string())?;
        inventory.extend(scanner.inventory);
    }
    let entry = out.join(entry);
    let rows = inventory
        .iter()
        .map(|(name, state)| format!("({name:?},{state:?})"))
        .collect::<Vec<_>>()
        .join(",");
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&entry)
        .map_err(|e| e.to_string())?;
    writeln!(
        file,
        "\npub const COREQ_TELEMETRY_COVERAGE: &[(&str,&str)] = &[{rows}];"
    )
    .map_err(|e| e.to_string())?;
    let manifest =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or("missing manifest directory")?);
    let base = source
        .strip_prefix(&manifest)
        .map_err(|_| "instrumented source must be inside the consumer manifest")?
        .to_string_lossy()
        .replace('\\', "/");
    writeln!(file,"pub const COREQ_TELEMETRY_SOURCE_ROOT:&str={:?}; pub const COREQ_TELEMETRY_SOURCE_BASE:&str={:?};",out.to_string_lossy().replace('\\',"/"),base).map_err(|e|e.to_string())?;
    Ok(Generated { entry, inventory })
}
fn walk(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for e in fs::read_dir(root).map_err(|e| e.to_string())? {
        let e = e.map_err(|e| e.to_string())?;
        let ty = e.file_type().map_err(|e| e.to_string())?;
        if ty.is_dir() {
            walk(&e.path(), files)?;
        } else if ty.is_file() && e.path().extension().is_some_and(|s| s == "rs") {
            files.push(e.path());
        }
    }
    Ok(())
}
struct Edit {
    start: usize,
    end: usize,
    text: String,
    depth: usize,
    close: bool,
}
struct Scanner<'a> {
    edits: Vec<Edit>,
    inventory: Vec<(String, String)>,
    namespace: Vec<String>,
    depth: usize,
    parent: &'a Path,
}
fn result(sig: &ReturnType) -> bool {
    matches!(sig,ReturnType::Type(_,ty) if matches!(ty.as_ref(),Type::Path(p) if p.path.segments.last().is_some_and(|s|s.ident=="Result"||s.ident=="Res")))
}
impl Scanner<'_> {
    fn insert(&mut self, at: usize, text: String, close: bool) {
        self.edits.push(Edit {
            start: at,
            end: at,
            text,
            depth: self.depth,
            close,
        });
    }
    fn wrap(&mut self, span: Span, prefix: &str, suffix: &str) {
        let r = span.byte_range();
        self.insert(r.start, prefix.into(), false);
        self.insert(r.end, suffix.into(), true);
    }
    fn method(&mut self, sig: &Signature, span: Span, body: &syn::Block) {
        let name = self
            .namespace
            .iter()
            .cloned()
            .chain(std::iter::once(sig.ident.to_string()))
            .collect::<Vec<_>>()
            .join(".");
        if sig.constness.is_some() {
            self.inventory.push((name, "const-evaluation".into()));
            return;
        }
        self.inventory.push((name.clone(), "instrumented".into()));
        if sig.asyncness.is_some() {
            self.insert(
                span.byte_range().start,
                format!("#[::tracing::instrument(name={name:?},skip_all)] "),
                false,
            );
        } else {
            self.insert(
                body.brace_token.span.open().byte_range().end,
                format!(
                    "let __coreq_telemetry_scope_{}=::tracing::info_span!({name:?}).entered();",
                    span.byte_range().start
                ),
                false,
            );
        }
        if result(&sig.output) {
            if let ReturnType::Type(_, ty) = &sig.output {
                let mut ty = (**ty).clone();
                EraseOpaque.visit_type_mut(&mut ty);
                self.exits(body, &ty);
            }
        }
    }
    fn nested_includes(&mut self, tokens: proc_macro2::TokenStream) {
        let tokens = tokens.into_iter().collect::<Vec<_>>();
        for w in tokens.windows(3) {
            if let (
                proc_macro2::TokenTree::Ident(name),
                proc_macro2::TokenTree::Punct(mark),
                proc_macro2::TokenTree::Group(group),
            ) = (&w[0], &w[1], &w[2])
            {
                if mark.as_char() == '!'
                    && ["include_str", "include_bytes", "include"]
                        .contains(&name.to_string().as_str())
                {
                    if let Ok(literal) = syn::parse2::<syn::LitStr>(group.stream()) {
                        let value = syn::LitStr::new(
                            &self.parent.join(literal.value()).to_string_lossy(),
                            literal.span(),
                        )
                        .into_token_stream()
                        .to_string();
                        let r = literal.span().byte_range();
                        self.edits.push(Edit {
                            start: r.start,
                            end: r.end,
                            text: value,
                            depth: self.depth,
                            close: false,
                        });
                    }
                }
            }
        }
        for token in tokens {
            if let proc_macro2::TokenTree::Group(group) = token {
                self.nested_includes(group.stream());
            }
        }
    }
    fn exits(&mut self, body: &syn::Block, ty: &Type) {
        self.depth += 1;
        let name = format!(
            "__coreq_telemetry_result_{}",
            body.span().byte_range().start
        );
        self.insert(
            body.brace_token.span.open().byte_range().end,
            format!("let {name}:{}={{", ty.to_token_stream()),
            false,
        );
        // Only the generated observer may be unreachable; the original body keeps its own lints.
        self.insert(body.brace_token.span.close().byte_range().start,format!("}};#[allow(unreachable_code)]{{::coreq::telemetry::capture::returned_result({name})}}"),true);
        self.depth += 1;
        Exits(self).visit_block(body);
        self.depth -= 2;
    }
}
impl<'a> Visit<'a> for Scanner<'_> {
    fn visit_item_mod(&mut self, m: &'a syn::ItemMod) {
        self.namespace.push(m.ident.to_string());
        visit::visit_item_mod(self, m);
        self.namespace.pop();
    }
    fn visit_item_impl(&mut self, i: &'a syn::ItemImpl) {
        let name = match i.self_ty.as_ref() {
            Type::Path(p) => p.path.segments.last().unwrap().ident.to_string(),
            _ => "impl".into(),
        };
        self.namespace.push(name);
        visit::visit_item_impl(self, i);
        self.namespace.pop();
    }
    fn visit_item_fn(&mut self, f: &'a syn::ItemFn) {
        self.method(&f.sig, f.span(), &f.block);
        if f.sig.constness.is_none() {
            self.namespace.push(f.sig.ident.to_string());
            self.depth += 2;
            visit::visit_item_fn(self, f);
            self.depth -= 2;
            self.namespace.pop();
        }
    }
    fn visit_impl_item_fn(&mut self, f: &'a syn::ImplItemFn) {
        self.method(&f.sig, f.span(), &f.block);
        if f.sig.constness.is_none() {
            self.namespace.push(f.sig.ident.to_string());
            self.depth += 2;
            visit::visit_impl_item_fn(self, f);
            self.depth -= 2;
            self.namespace.pop();
        }
    }
    fn visit_trait_item_fn(&mut self, f: &'a syn::TraitItemFn) {
        if let Some(body) = &f.default {
            self.method(&f.sig, f.span(), body);
        }
        visit::visit_trait_item_fn(self, f);
    }
    fn visit_expr_closure(&mut self, c: &'a syn::ExprClosure) {
        let classification = if matches!(c.output, ReturnType::Default) {
            "unclassified"
        } else {
            "observed"
        };
        let name = format!(
            "{}.callback{}",
            self.namespace.join("."),
            c.span().byte_range().start
        );
        self.inventory
            .push((name.clone(), "instrumented-callback".into()));
        if c.asyncness.is_some() {
            self.wrap(
                c.body.span(),
                "{::tracing::Instrument::instrument(async {",
                &format!(
                    "}},::tracing::info_span!({name:?},coreq_result={classification:?})).await}}"
                ),
            );
        } else {
            let guard = format!(
                "let __coreq_telemetry_scope_{}=::tracing::info_span!({name:?},coreq_result={classification:?}).entered();",
                c.span().byte_range().start
            );
            if let Expr::Block(body) = c.body.as_ref() {
                self.insert(
                    body.block.brace_token.span.open().byte_range().end,
                    guard,
                    false,
                );
            } else {
                self.wrap(c.body.span(), &format!("{{{guard}"), "}");
            }
        }
        self.depth += 1;
        if result(&c.output) {
            if let ReturnType::Type(_, ty) = &c.output {
                if let Expr::Block(body) = c.body.as_ref() {
                    let mut ty = (**ty).clone();
                    EraseOpaque.visit_type_mut(&mut ty);
                    self.exits(&body.block, &ty);
                } else {
                    self.wrap(
                        c.body.span(),
                        "::coreq::telemetry::capture::returned_result(",
                        ")",
                    );
                    self.depth += 1;
                    Exits(self).visit_expr(&c.body);
                    self.depth -= 1;
                }
            }
        }
        visit::visit_expr_closure(self, c);
        self.depth -= 1;
    }

    fn visit_expr_async(&mut self, a: &'a syn::ExprAsync) {
        let name = format!(
            "{}.future{}",
            self.namespace.join("."),
            a.span().byte_range().start
        );
        self.inventory
            .push((name.clone(), "instrumented-callback".into()));
        self.wrap(
            a.span(),
            "::tracing::Instrument::instrument(",
            &format!(",::tracing::info_span!({name:?},coreq_result=\"unclassified\"))"),
        );
        self.depth += 1;
        visit::visit_expr_async(self, a);
        self.depth -= 1;
    }
    fn visit_macro(&mut self, m: &'a syn::Macro) {
        if m.path
            .get_ident()
            .is_some_and(|n| n == "include_str" || n == "include_bytes" || n == "include")
        {
            if let Ok(literal) = syn::parse2::<syn::LitStr>(m.tokens.clone()) {
                let path = self.parent.join(literal.value());
                let value = syn::LitStr::new(&path.to_string_lossy(), literal.span())
                    .into_token_stream()
                    .to_string();
                let r = literal.span().byte_range();
                self.edits.push(Edit {
                    start: r.start,
                    end: r.end,
                    text: value,
                    depth: self.depth,
                    close: false,
                });
            }
        }
        self.nested_includes(m.tokens.clone());
        visit::visit_macro(self, m);
    }
}
struct Exits<'s, 'p>(&'s mut Scanner<'p>);
impl<'a> Visit<'a> for Exits<'_, '_> {
    fn visit_expr_closure(&mut self, _: &'a syn::ExprClosure) {}
    fn visit_expr_async(&mut self, _: &'a syn::ExprAsync) {}
    fn visit_item_fn(&mut self, _: &'a syn::ItemFn) {}
    fn visit_expr_return(&mut self, r: &'a syn::ExprReturn) {
        if let Some(expr) = &r.expr {
            self.0.wrap(
                expr.span(),
                "::coreq::telemetry::capture::returned_result(",
                ")",
            );
        }
        self.0.depth += 1;
        visit::visit_expr_return(self, r);
        self.0.depth -= 1;
    }
    fn visit_expr_try(&mut self, t: &'a syn::ExprTry) {
        self.0.wrap(
            t.expr.span(),
            "::coreq::telemetry::capture::returned_result(",
            ")",
        );
        self.0.depth += 1;
        visit::visit_expr_try(self, t);
        self.0.depth -= 1;
    }
}

struct EraseOpaque;
impl syn::visit_mut::VisitMut for EraseOpaque {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if matches!(ty, Type::ImplTrait(_)) {
            *ty = syn::parse_quote!(_);
        } else {
            syn::visit_mut::visit_type_mut(self, ty);
        }
    }
}
