## Why are we rewriting MinIO in Rust?

## 1.1 AGPL v3.0 Protocol Pitfalls and Risks

Due to the many protocol pitfalls of the AGPL v3.0, many companies encounter a large number of cross-border patent disputes in the process of globalization.

Therefore, we will use Apache 2.0 open source protocol to re-implement an object store using RustFS.

This will help users around the world deal with the potential legal risks and disputes of AGPL v3.0.


## 1.2 Advantages of Rust

In addition, Rust is currently the world's most popular and secure development language.

The Rust language has been widely noticed and hotly debated for the following reasons:
1. **Safety**: one of the core selling points of Rust is its memory safety. rust prevents memory safety issues such as null pointers, data contention, etc. at compile time without the need for a garbage collection mechanism, through mechanisms such as ownership, borrowing, and lifetimes, which is particularly important for systems programming. especially important for systems programming.
2. **PERFORMANCE**: The Rust compiler generates efficient machine code that is comparable to traditional systems programming languages such as C/C++, making Rust useful in scenarios where high performance is required.
3. **Modern Language Features**: Rust has many of the features of modern programming languages, such as pattern matching, type derivation, concurrency support, and zero-cost abstraction, which improve development efficiency and code quality.
4. **Cross-platform**: Rust supports cross-platform development, and can be compiled and run on multiple operating systems and hardware platforms, including embedded systems.
5. **Systems Programming**: Rust is seen as a modern alternative to C/C++, especially in situations where secure and reliable systems software needs to be written.
6. **WebAssembly**: Rust can be compiled into WebAssembly, which allows Rust code to run on web pages, opening up new possibilities for web development.
7. **Corporate Support**: Many large technology companies, such as Mozilla (the creator of Rust), Dropbox, Coursera, Microsoft, etc., use or support Rust, which increases trust in the language.
8. **Open Source Projects**: Many well-known open source projects, such as Firefox, TensorFlow, Deno, and ripgrep, use Rust, and the success of these projects has increased Rust's popularity.
9. **Continuous Development**: Rust's development team continues to release new versions, each bringing new features and improvements, demonstrating that Rust is an active and evolving language.

Because of these features, we decided to rewrite a distributed object store using Rust.


## 1.3 Does RustFS support distribution?

Yes.


## 1.4 Is RustFS easy to use?

RustFS is very easy to use. RustFS can be started with a single command.

