//! [`HookPoint`] re-export.
//!
//! The hook-point *vocabulary* moved into `gt-module` in `hq-mod-hooks.3` so a
//! module can declare its subscriptions in [`Capability`](gt_module::Capability)
//! without `gt-module` depending on this crate (which would be a cycle, and would
//! drag the async `dyn` handler into the sync core). `gt-hooks` re-exports it here
//! so handler authors keep importing `HookPoint` from one place alongside
//! [`HookHandler`](crate::HookHandler) and [`HookRegistry`](crate::HookRegistry).

pub use gt_module::HookPoint;
