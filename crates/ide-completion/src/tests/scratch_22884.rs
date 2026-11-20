//! Scratch tests for issue 22884. DELETE BEFORE COMMIT.
use expect_test::expect;

use crate::tests::check;

#[test]
fn scratch_state_deref_completion() {
    check(
        r#"
//- proc_macros: identity
//- minicore: deref
use core::ops::Deref;

pub struct PgPool;
pub struct Database { pub pool: PgPool }
pub struct AppState { pub database: Database }

pub struct State<'a, T>(pub &'a T);
impl<'a, T> Deref for State<'a, T> {
    type Target = T;
    fn deref(&self) -> &T { self.0 }
}

#[proc_macros::identity]
fn greet(state: State<'_, AppState>) {
    let a = state.$0;
}
"#,
        expect![[r#""#]],
    );
}

#[test]
fn scratch_arc_state() {
    check(
        r#"
//- proc_macros: identity
//- minicore: deref
use core::ops::Deref;
use std::sync::Arc;

pub struct PgPool;
pub struct Database { pub pool: Arc<PgPool> }
pub struct AppState { pub database: Database }

pub struct State<'a, T>(pub &'a T);
impl<'a, T> Deref for State<'a, T> {
    type Target = T;
    fn deref(&self) -> &T { self.0 }
}

#[proc_macros::identity]
fn greet(state: State<'_, Arc<AppState>>) {
    let a = state.$0;
}
"#,
        expect![[r#""#]],
    );
}
