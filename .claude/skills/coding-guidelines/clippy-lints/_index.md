# Clippy Lint → Rule Mapping

| Clippy Lint | Category | Fix |
|-------------|----------|-----|
| `unwrap_used` | Error | Use `?` or `expect()` |
| `needless_clone` | Perf | Use reference |
| `await_holding_lock` | Async | Scope guard before await |
| `linkedlist` | Perf | Use Vec/VecDeque |
| `wildcard_imports` | Style | Explicit imports |
| `missing_safety_doc` | Safety | Add `# Safety` doc |
| `undocumented_unsafe_blocks` | Safety | Add `// SAFETY:` |
| `transmute_ptr_to_ptr` | Safety | Use `pointer::cast()` |
| `large_stack_arrays` | Mem | Use Vec or Box |
| `too_many_arguments` | Design | Use struct params |

For unsafe-related lints → see `unsafe-checker` skill.
