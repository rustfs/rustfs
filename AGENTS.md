# RustFS Project AI Agents Rules

## 🚨🚨🚨 CRITICAL DEVELOPMENT RULES - ZERO TOLERANCE 🚨🚨🚨

### ⛔️ ABSOLUTE PROHIBITION: NEVER COMMIT DIRECTLY TO MASTER/MAIN BRANCH ⛔️

**🔥 THIS IS THE MOST CRITICAL RULE - VIOLATION WILL RESULT IN IMMEDIATE REVERSAL 🔥**

- **🚫 ZERO DIRECT COMMITS TO MAIN/MASTER BRANCH - ABSOLUTELY FORBIDDEN**
- **🚫 ANY DIRECT COMMIT TO MAIN BRANCH MUST BE IMMEDIATELY REVERTED**
- **🚫 NO EXCEPTIONS FOR HOTFIXES, EMERGENCIES, OR URGENT CHANGES**
- **🚫 NO EXCEPTIONS FOR SMALL CHANGES, TYPOS, OR DOCUMENTATION UPDATES**
- **🚫 NO EXCEPTIONS FOR ANYONE - MAINTAINERS, CONTRIBUTORS, OR ADMINS**

### 📋 MANDATORY WORKFLOW - STRICTLY ENFORCED

**EVERY SINGLE CHANGE MUST FOLLOW THIS WORKFLOW:**

1. **Check current branch**: `git branch` (MUST NOT be on main/master)
2. **Switch to main**: `git checkout main`
3. **Pull latest**: `git pull origin main`
4. **Create feature branch**: `git checkout -b feat/your-feature-name`
5. **Make changes ONLY on feature branch**
6. **Test thoroughly before committing**
7. **Commit and push to feature branch**: `git push origin feat/your-feature-name`
8. **Create Pull Request**: Use `gh pr create` (MANDATORY)
9. **Wait for PR approval**: NO self-merging allowed
10. **Merge through GitHub interface**: ONLY after approval

### 🔒 ENFORCEMENT MECHANISMS

- **Branch protection rules**: Main branch is protected
- **Pre-commit hooks**: Will block direct commits to main
- **CI/CD checks**: All PRs must pass before merging
- **Code review requirement**: At least one approval needed
- **Automated reversal**: Direct commits to main will be automatically reverted

## 🎯 Core Development Principles (HIGHEST PRIORITY)

### Philosophy

#### Core Beliefs

- **Incremental progress over big bangs** - Small changes that compile and pass tests
- **Learning from existing code** - Study and plan before implementing
- **Pragmatic over dogmatic** - Adapt to project reality
- **Clear intent over clever code** - Be boring and obvious

#### Simplicity Means

- Single responsibility per function/class
- Avoid premature abstractions
- No clever tricks - choose the boring solution
- If you need to explain it, it's too complex

### Process

#### 1. Planning & Staging

Break complex work into 3-5 stages. Document in `IMPLEMENTATION_PLAN.md`:

```markdown
## Stage N: [Name]
**Goal**: [Specific deliverable]
**Success Criteria**: [Testable outcomes]
**Tests**: [Specific test cases]
**Status**: [Not Started|In Progress|Complete]
```

- Update status as you progress
- Remove file when all stages are done

#### 2. Implementation Flow

1. **Understand** - Study existing patterns in codebase
2. **Test** - Write test first (red)
3. **Implement** - Minimal code to pass (green)
4. **Refactor** - Clean up with tests passing
5. **Commit** - With clear message linking to plan

#### 3. When Stuck (After 3 Attempts)

**CRITICAL**: Maximum 3 attempts per issue, then STOP.

1. **Document what failed**:
   - What you tried
   - Specific error messages
   - Why you think it failed

2. **Research alternatives**:
   - Find 2-3 similar implementations
   - Note different approaches used

3. **Question fundamentals**:
   - Is this the right abstraction level?
   - Can this be split into smaller problems?
   - Is there a simpler approach entirely?

4. **Try different angle**:
   - Different library/framework feature?
   - Different architectural pattern?
   - Remove abstraction instead of adding?

### Technical Standards

#### Architecture Principles

- **Composition over inheritance** - Use dependency injection
- **Interfaces over singletons** - Enable testing and flexibility
- **Explicit over implicit** - Clear data flow and dependencies
- **Test-driven when possible** - Never disable tests, fix them

#### Code Quality

- **Every commit must**:
  - Compile successfully
  - Pass all existing tests
  - Include tests for new functionality
  - Follow project formatting/linting

- **Before committing**:
  - Run formatters/linters
  - Self-review changes
  - Ensure commit message explains "why"

#### Error Handling

- Fail fast with descriptive messages
- Include context for debugging
- Handle errors at appropriate level
- Never silently swallow exceptions

### Decision Framework

When multiple valid approaches exist, choose based on:

1. **Testability** - Can I easily test this?
2. **Readability** - Will someone understand this in 6 months?
3. **Consistency** - Does this match project patterns?
4. **Simplicity** - Is this the simplest solution that works?
5. **Reversibility** - How hard to change later?

### Project Integration

#### Learning the Codebase

- Find 3 similar features/components
- Identify common patterns and conventions
- Use same libraries/utilities when possible
- Follow existing test patterns

#### Tooling

- Use project's existing build system
- Use project's test framework
- Use project's formatter/linter settings
- Don't introduce new tools without strong justification

### Quality Gates

#### Definition of Done

- [ ] Tests written and passing
- [ ] Code follows project conventions
- [ ] No linter/formatter warnings
- [ ] Commit messages are clear
- [ ] Implementation matches plan
- [ ] No TODOs without issue numbers

#### Test Guidelines

- Test behavior, not implementation
- One assertion per test when possible
- Clear test names describing scenario
- Use existing test utilities/helpers
- Tests should be deterministic

### Important Reminders

**NEVER**:

- Use `--no-verify` to bypass commit hooks
- Disable tests instead of fixing them
- Commit code that doesn't compile
- Make assumptions - verify with existing code

**ALWAYS**:

- Commit working code incrementally
- Update plan documentation as you go
- Learn from existing implementations
- Stop after 3 failed attempts and reassess

## 🚫 Competitor Keywords Prohibition

### Strictly Forbidden Keywords

**CRITICAL**: The following competitor keywords are absolutely forbidden in any code, documentation, comments, or project files:

- **minio** (and any variations like MinIO, MINIO)
- **aws-s3** (when referring to competing implementations)
- **ceph** (and any variations like Ceph, CEPH)
- **swift** (OpenStack Swift)
- **glusterfs** (and any variations like GlusterFS, Gluster)
- **seaweedfs** (and any variations like SeaweedFS, Seaweed)
- **garage** (and any variations like Garage)
- **zenko** (and any variations like Zenko)
- **scality** (and any variations like Scality)

### Enforcement

- **Code Review**: All PRs will be checked for competitor keywords
- **Automated Scanning**: CI/CD pipeline will scan for forbidden keywords
- **Immediate Rejection**: Any PR containing competitor keywords will be immediately rejected
- **Documentation**: All documentation must use generic terms like "S3-compatible storage" instead of specific competitor names

### Acceptable Alternatives

Instead of competitor names, use these generic terms:

- "S3-compatible storage system"
- "Object storage solution"
- "Distributed storage platform"
- "Cloud storage service"
- "Storage backend"

## Project Overview

RustFS is a high-performance distributed object storage system written in Rust, compatible with S3 API. The project adopts a modular architecture, supporting erasure coding storage, multi-tenant management, observability, and other enterprise-level features.

## Core Architecture Principles

### 1. Modular Design

- Project uses Cargo workspace structure, containing multiple independent crates
- Core modules: `rustfs` (main service), `ecstore` (erasure coding storage), `common` (shared components)
- Functional modules: `iam` (identity management), `madmin` (management interface), `crypto` (encryption), etc.
- Tool modules: `cli` (command line tool), `crates/*` (utility libraries)

### 2. Asynchronous Programming Pattern

- Comprehensive use of `tokio` async runtime
- Prioritize `async/await` syntax
- Use `async-trait` for async methods in traits
- Avoid blocking operations, use `spawn_blocking` when necessary

### 3. Error Handling Strategy

- **Use modular, type-safe error handling with `thiserror`**
- Each module should define its own error type using `thiserror::Error` derive macro
- Support error chains and context information through `#[from]` and `#[source]` attributes
- Use `Result<T>` type aliases for consistency within each module
- Error conversion between modules should use explicit `From` implementations
- Follow the pattern: `pub type Result<T> = core::result::Result<T, Error>`
- Use `#[error("description")]` attributes for clear error messages
- Support error downcasting when needed through `other()` helper methods
- Implement `Clone` for errors when required by the domain logic

## Code Style Guidelines

### 1. Formatting Configuration

```toml
max_width = 130
fn_call_width = 90
single_line_let_else_max_width = 100
```

### 2. **🔧 MANDATORY Code Formatting Rules**

**CRITICAL**: All code must be properly formatted before committing. This project enforces strict formatting standards to maintain code consistency and readability.

#### Pre-commit Requirements (MANDATORY)

Before every commit, you **MUST**:

1. **Format your code**:

   ```bash
   cargo fmt --all
   ```

2. **Verify formatting**:

   ```bash
   cargo fmt --all --check
   ```

3. **Pass clippy checks**:

   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```

4. **Ensure compilation**:

   ```bash
   cargo check --all-targets
   ```

#### Quick Commands

Use these convenient Makefile targets for common tasks:

```bash
# Format all code
make fmt

# Check if code is properly formatted
make fmt-check

# Run clippy checks
make clippy

# Run compilation check
make check

# Run tests
make test

# Run all pre-commit checks (format + clippy + check + test)
make pre-commit

# Setup git hooks (one-time setup)
make setup-hooks
```

### 3. Naming Conventions

- Use `snake_case` for functions, variables, modules
- Use `PascalCase` for types, traits, enums
- Constants use `SCREAMING_SNAKE_CASE`
- Global variables prefix `GLOBAL_`, e.g., `GLOBAL_Endpoints`
- Use meaningful and descriptive names for variables, functions, and methods
- Avoid meaningless names like `temp`, `data`, `foo`, `bar`, `test123`
- Choose names that clearly express the purpose and intent

### 4. Type Declaration Guidelines

- **Prefer type inference over explicit type declarations** when the type is obvious from context
- Let the Rust compiler infer types whenever possible to reduce verbosity and improve maintainability
- Only specify types explicitly when:
  - The type cannot be inferred by the compiler
  - Explicit typing improves code clarity and readability
  - Required for API boundaries (function signatures, public struct fields)
  - Needed to resolve ambiguity between multiple possible types

### 5. Documentation Comments

- Public APIs must have documentation comments
- Use `///` for documentation comments
- Complex functions add `# Examples` and `# Parameters` descriptions
- Error cases use `# Errors` descriptions
- Always use English for all comments and documentation
- Avoid meaningless comments like "debug 111" or placeholder text

### 6. Import Guidelines

- Standard library imports first
- Third-party crate imports in the middle
- Project internal imports last
- Group `use` statements with blank lines between groups

## Asynchronous Programming Guidelines

- Comprehensive use of `tokio` async runtime
- Prioritize `async/await` syntax
- Use `async-trait` for async methods in traits
- Avoid blocking operations, use `spawn_blocking` when necessary
- Use `Arc` and `Mutex`/`RwLock` for shared state management
- Prioritize async locks from `tokio::sync`
- Avoid holding locks for long periods

## Logging and Tracing Guidelines

- Use `#[tracing::instrument(skip(self, data))]` for function tracing
- Log levels: `error!` (system errors), `warn!` (warnings), `info!` (business info), `debug!` (development), `trace!` (detailed paths)
- Use structured logging with key-value pairs for better observability

## Error Handling Guidelines

- Use `thiserror` for module-specific error types
- Support error chains and context information through `#[from]` and `#[source]` attributes
- Use `Result<T>` type aliases for consistency within each module
- Error conversion between modules should use explicit `From` implementations
- Follow the pattern: `pub type Result<T> = core::result::Result<T, Error>`
- Use `#[error("description")]` attributes for clear error messages
- Support error downcasting when needed through `other()` helper methods
- Implement `Clone` for errors when required by the domain logic

## Performance Optimization Guidelines

- Use `Bytes` instead of `Vec<u8>` for zero-copy operations
- Avoid unnecessary cloning, use reference passing
- Use `Arc` for sharing large objects
- Use `join_all` for concurrent operations
- Use `LazyLock` for global caching
- Implement LRU cache to avoid memory leaks

## Testing Guidelines

- Write meaningful test cases that verify actual functionality
- Avoid placeholder or debug content like "debug 111", "test test", etc.
- Use descriptive test names that clearly indicate what is being tested
- Each test should have a clear purpose and verify specific behavior
- Test data should be realistic and representative of actual use cases
- Use `e2e_test` module for end-to-end testing
- Simulate real storage environments

## Cross-Platform Compatibility Guidelines

- **Always consider multi-platform and different CPU architecture compatibility** when writing code
- Support major architectures: x86_64, aarch64 (ARM64), and other target platforms
- Use conditional compilation for architecture-specific code
- Use feature flags for platform-specific dependencies
- Provide fallback implementations for unsupported platforms
- Test on multiple architectures in CI/CD pipeline
- Use explicit byte order conversion when dealing with binary data
- Prefer `to_le_bytes()`, `from_le_bytes()` for consistent little-endian format
- Use portable SIMD libraries like `wide` or `packed_simd`
- Provide fallback implementations for non-SIMD architectures

## Security Guidelines

- Disable `unsafe` code (workspace.lints.rust.unsafe_code = "deny")
- Use `rustls` instead of `openssl`
- Use IAM system for permission checks
- Validate input parameters properly
- Implement appropriate permission checks
- Avoid information leakage

## Configuration Management Guidelines

- Use `RUSTFS_` prefix for environment variables
- Support both configuration files and environment variables
- Provide reasonable default values
- Use `serde` for configuration serialization/deserialization

## Dependency Management Guidelines

- Manage versions uniformly at workspace level
- Use `workspace = true` to inherit configuration
- Use feature flags for optional dependencies
- Don't introduce new tools without strong justification

## Deployment and Operations Guidelines

- Provide Dockerfile and docker-compose configuration
- Support multi-stage builds to optimize image size
- Integrate OpenTelemetry for distributed tracing
- Support Prometheus metrics collection
- Provide Grafana dashboards
- Implement health check endpoints

## Code Review Checklist

### 1. **Code Formatting and Quality (MANDATORY)**

- [ ] **Code is properly formatted** (`cargo fmt --all --check` passes)
- [ ] **All clippy warnings are resolved** (`cargo clippy --all-targets --all-features -- -D warnings` passes)
- [ ] **Code compiles successfully** (`cargo check --all-targets` passes)
- [ ] **Pre-commit hooks are working** and all checks pass
- [ ] **No formatting-related changes** mixed with functional changes (separate commits)

### 2. Functionality

- [ ] Are all error cases properly handled?
- [ ] Is there appropriate logging?
- [ ] Is there necessary test coverage?

### 3. Performance

- [ ] Are unnecessary memory allocations avoided?
- [ ] Are async operations used correctly?
- [ ] Are there potential deadlock risks?

### 4. Security

- [ ] Are input parameters properly validated?
- [ ] Are there appropriate permission checks?
- [ ] Is information leakage avoided?

### 5. Cross-Platform Compatibility

- [ ] Does the code work on different CPU architectures (x86_64, aarch64)?
- [ ] Are platform-specific features properly gated with conditional compilation?
- [ ] Is byte order handling correct for binary data?
- [ ] Are there appropriate fallback implementations for unsupported platforms?

### 6. Code Commits and Documentation

- [ ] Does it comply with [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)?
- [ ] Are commit messages concise and under 72 characters for the title line?
- [ ] Commit titles should be concise and in English, avoid Chinese
- [ ] Is PR description provided in copyable markdown format for easy copying?

### 7. Competitor Keywords Check

- [ ] No competitor keywords found in code, comments, or documentation
- [ ] All references use generic terms like "S3-compatible storage"
- [ ] No specific competitor product names mentioned

## Domain-Specific Guidelines

### 1. Storage Operations

- All storage operations must support erasure coding
- Implement read/write quorum mechanisms
- Support data integrity verification

### 2. Network Communication

- Use gRPC for internal service communication
- HTTP/HTTPS support for S3-compatible API
- Implement connection pooling and retry mechanisms

### 3. Metadata Management

- Use FlatBuffers for serialization
- Support version control and migration
- Implement metadata caching

## Branch Management and Development Workflow

### Branch Management

- **🚨 CRITICAL: NEVER modify code directly on main or master branch - THIS IS ABSOLUTELY FORBIDDEN 🚨**
- **⚠️ ANY DIRECT COMMITS TO MASTER/MAIN WILL BE REJECTED AND MUST BE REVERTED IMMEDIATELY ⚠️**
- **🔒 ALL CHANGES MUST GO THROUGH PULL REQUESTS - NO DIRECT COMMITS TO MAIN UNDER ANY CIRCUMSTANCES 🔒**
- **Always work on feature branches - NO EXCEPTIONS**
- Always check the AGENTS.md file before starting to ensure you understand the project guidelines
- **MANDATORY workflow for ALL changes:**
   1. `git checkout main` (switch to main branch)
   2. `git pull` (get latest changes)
   3. `git checkout -b feat/your-feature-name` (create and switch to feature branch)
   4. Make your changes ONLY on the feature branch
   5. Test thoroughly before committing
   6. Commit and push to the feature branch
   7. **Create a pull request for code review - THIS IS THE ONLY WAY TO MERGE TO MAIN**
   8. **Wait for PR approval before merging - NEVER merge your own PRs without review**
- Use descriptive branch names following the pattern: `feat/feature-name`, `fix/issue-name`, `refactor/component-name`, etc.
- **Double-check current branch before ANY commit: `git branch` to ensure you're NOT on main/master**
- **Pull Request Requirements:**
  - All changes must be submitted via PR regardless of size or urgency
  - PRs must include comprehensive description and testing information
  - PRs must pass all CI/CD checks before merging
  - PRs require at least one approval from code reviewers
  - Even hotfixes and emergency changes must go through PR process
- **Enforcement:**
  - Main branch should be protected with branch protection rules
  - Direct pushes to main should be blocked by repository settings
  - Any accidental direct commits to main must be immediately reverted via PR

### Development Workflow

## 🎯 **Core Development Principles**

- **🔴 Every change must be precise - don't modify unless you're confident**
  - Carefully analyze code logic and ensure complete understanding before making changes
  - When uncertain, prefer asking users or consulting documentation over blind modifications
  - Use small iterative steps, modify only necessary parts at a time
  - Evaluate impact scope before changes to ensure no new issues are introduced

- **🚀 GitHub PR creation prioritizes gh command usage**
  - Prefer using `gh pr create` command to create Pull Requests
  - Avoid having users manually create PRs through web interface
  - Provide clear and professional PR titles and descriptions
  - Using `gh` commands ensures better integration and automation

## 📝 **Code Quality Requirements**

- Use English for all code comments, documentation, and variable names
- Write meaningful and descriptive names for variables, functions, and methods
- Avoid meaningless test content like "debug 111" or placeholder values
- Before each change, carefully read the existing code to ensure you understand the code structure and implementation, do not break existing logic implementation, do not introduce new issues
- Ensure each change provides sufficient test cases to guarantee code correctness
- Do not arbitrarily modify numbers and constants in test cases, carefully analyze their meaning to ensure test case correctness
- When writing or modifying tests, check existing test cases to ensure they have scientific naming and rigorous logic testing, if not compliant, modify test cases to ensure scientific and rigorous testing
- **Before committing any changes, run `cargo clippy --all-targets --all-features -- -D warnings` to ensure all code passes Clippy checks**
- After each development completion, first git add . then git commit -m "feat: feature description" or "fix: issue description", ensure compliance with [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
- **Keep commit messages concise and under 72 characters** for the title line, use body for detailed explanations if needed
- After each development completion, first git push to remote repository
- After each change completion, summarize the changes, do not create summary files, provide a brief change description, ensure compliance with [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
- Provide change descriptions needed for PR in the conversation, ensure compliance with [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
- **Always provide PR descriptions in English** after completing any changes, including:
  - Clear and concise title following Conventional Commits format
  - Detailed description of what was changed and why
  - List of key changes and improvements
  - Any breaking changes or migration notes if applicable
  - Testing information and verification steps
- **Provide PR descriptions in copyable markdown format** enclosed in code blocks for easy one-click copying

## 🚫 AI Documentation Generation Restrictions

### Forbidden Summary Documents

- **Strictly forbidden to create any form of AI-generated summary documents**
- **Do not create documents containing large amounts of emoji, detailed formatting tables and typical AI style**
- **Do not generate the following types of documents in the project:**
  - Benchmark summary documents (BENCHMARK*.md)
  - Implementation comparison analysis documents (IMPLEMENTATION_COMPARISON*.md)
  - Performance analysis report documents
  - Architecture summary documents
  - Feature comparison documents
  - Any documents with large amounts of emoji and formatted content
- **If documentation is needed, only create when explicitly requested by the user, and maintain a concise and practical style**
- **Documentation should focus on actually needed information, avoiding excessive formatting and decorative content**
- **Any discovered AI-generated summary documents should be immediately deleted**

### Allowed Documentation Types

- README.md (project introduction, keep concise)
- Technical documentation (only create when explicitly needed)
- User manual (only create when explicitly needed)
- API documentation (generated from code)
- Changelog (CHANGELOG.md)

These rules should serve as guiding principles when developing the RustFS project, ensuring code quality, performance, and maintainability.
