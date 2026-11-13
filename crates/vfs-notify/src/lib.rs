//! An implementation of `loader::Handle`, based on `walkdir` and `notify`.
//!
//! The file watching bits here are untested and quite probably buggy. For this
//! reason, by default we don't watch files and rely on editor's file watching
//! capabilities.
//!
//! Hopefully, one day a reliable file watching/walking crate appears on
//! crates.io, and we can reduce this to trivial glue code.

use std::{
    fs,
    path::{Component, Path},
    sync::atomic::AtomicUsize,
};

use crossbeam_channel::{Receiver, Sender, select, unbounded};
use notify::{
    Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{AccessKind, ModifyKind, RenameMode},
};
use paths::{AbsPath, AbsPathBuf, Utf8PathBuf};
use rayon::iter::{IndexedParallelIterator as _, IntoParallelIterator as _, ParallelIterator};
use rustc_hash::FxHashSet;
use vfs::loader::{self, LoadingProgress};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct NotifyHandle {
    // Relative order of fields below is significant.
    sender: Sender<Message>,
    _thread: stdx::thread::JoinHandle,
}

#[derive(Debug)]
enum Message {
    Config(loader::Config),
    Invalidate(AbsPathBuf),
}

impl loader::Handle for NotifyHandle {
    fn spawn(sender: loader::Sender) -> NotifyHandle {
        let actor = NotifyActor::new(sender);
        let (sender, receiver) = unbounded::<Message>();
        let thread = stdx::thread::Builder::new(stdx::thread::ThreadIntent::Worker, "VfsLoader")
            .spawn(move || actor.run(receiver))
            .expect("failed to spawn thread");
        NotifyHandle { sender, _thread: thread }
    }

    fn set_config(&mut self, config: loader::Config) {
        self.sender.send(Message::Config(config)).unwrap();
    }

    fn invalidate(&mut self, path: AbsPathBuf) {
        self.sender.send(Message::Invalidate(path)).unwrap();
    }

    fn load_sync(&mut self, path: &AbsPath) -> Option<Vec<u8>> {
        read(path)
    }
}

type NotifyEvent = notify::Result<notify::Event>;

struct NotifyActor {
    sender: loader::Sender,
    watched_file_entries: FxHashSet<AbsPathBuf>,
    watched_dir_entries: Vec<loader::Directories>,
    seen_paths: FxHashSet<AbsPathBuf>,
    // Drop order is significant.
    watcher: Option<(RecommendedWatcher, Receiver<NotifyEvent>)>,
}

#[derive(Debug)]
enum Event {
    Message(Message),
    NotifyEvent(NotifyEvent),
}

impl NotifyActor {
    fn new(sender: loader::Sender) -> NotifyActor {
        NotifyActor {
            sender,
            watched_dir_entries: Vec::new(),
            watched_file_entries: FxHashSet::default(),
            seen_paths: FxHashSet::default(),
            watcher: None,
        }
    }

    fn next_event(&self, receiver: &Receiver<Message>) -> Option<Event> {
        let Some((_, watcher_receiver)) = &self.watcher else {
            return receiver.recv().ok().map(Event::Message);
        };

        select! {
            recv(receiver) -> it => it.ok().map(Event::Message),
            recv(watcher_receiver) -> it => Some(Event::NotifyEvent(it.unwrap())),
        }
    }

    fn run(mut self, inbox: Receiver<Message>) {
        while let Some(event) = self.next_event(&inbox) {
            tracing::debug!(?event, "vfs-notify event");
            match event {
                Event::Message(msg) => match msg {
                    Message::Config(config) => {
                        self.watcher = None;
                        if !config.watch.is_empty() {
                            let (watcher_sender, watcher_receiver) = unbounded();
                            let watcher = log_notify_error(RecommendedWatcher::new(
                                move |event| {
                                    // we don't care about the error. If sending fails that usually
                                    // means we were dropped, so unwrapping will just add to the
                                    // panic noise.
                                    _ = watcher_sender.send(event);
                                },
                                Config::default(),
                            ));
                            self.watcher = watcher.map(|it| (it, watcher_receiver));
                        }

                        let config_version = config.version;

                        let n_total = config.load.len();
                        self.watched_dir_entries.clear();
                        self.watched_file_entries.clear();
                        self.seen_paths.clear();

                        self.send(loader::Message::Progress {
                            n_total,
                            n_done: LoadingProgress::Started,
                            config_version,
                            dir: None,
                        });

                        let (entry_tx, entry_rx) = unbounded();
                        let (watch_tx, watch_rx) = unbounded();
                        let processed = AtomicUsize::new(0);

                        config.load.into_par_iter().enumerate().for_each(|(i, entry)| {
                            let do_watch = config.watch.contains(&i);
                            if do_watch {
                                _ = entry_tx.send(entry.clone());
                            }
                            let files = Self::load_entry(
                                |f| _ = watch_tx.send(f.to_owned()),
                                entry,
                                do_watch,
                                |file| {
                                    self.send(loader::Message::Progress {
                                        n_total,
                                        n_done: LoadingProgress::Progress(
                                            processed.load(std::sync::atomic::Ordering::Relaxed),
                                        ),
                                        dir: Some(file),
                                        config_version,
                                    });
                                },
                            );
                            self.send(loader::Message::Loaded { files });
                            self.send(loader::Message::Progress {
                                n_total,
                                n_done: LoadingProgress::Progress(
                                    processed.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1,
                                ),
                                config_version,
                                dir: None,
                            });
                        });

                        drop(watch_tx);
                        for path in watch_rx {
                            self.watch(&path);
                        }

                        drop(entry_tx);
                        for entry in entry_rx {
                            match entry {
                                loader::Entry::Files(files) => {
                                    self.watched_file_entries.extend(files)
                                }
                                loader::Entry::Directories(dir) => {
                                    self.watched_dir_entries.push(dir)
                                }
                            }
                        }

                        self.send(loader::Message::Progress {
                            n_total,
                            n_done: LoadingProgress::Finished,
                            config_version,
                            dir: None,
                        });
                    }
                    Message::Invalidate(path) => {
                        let contents = read_exactly_named(path.as_path());
                        let files = vec![(path, contents)];
                        self.send(loader::Message::Changed { files });
                    }
                },
                Event::NotifyEvent(event) => {
                    if let Some(event) = log_notify_error(event)
                        && let EventKind::Create(_)
                        | EventKind::Modify(_)
                        | EventKind::Remove(_)
                        | EventKind::Access(AccessKind::Open(_)) = event.kind
                    {
                        let abs_paths: Vec<(usize, AbsPathBuf)> = event
                            .paths
                            .into_iter()
                            .enumerate()
                            .filter_map(|(idx, path)| {
                                Some((
                                    idx,
                                    AbsPathBuf::try_from(Utf8PathBuf::from_path_buf(path).ok()?)
                                        .expect("path is absolute"),
                                ))
                            })
                            .collect();

                        let mut saw_new_file = false;
                        for (_, abs_path) in &abs_paths {
                            if self.seen_paths.insert(abs_path.clone()) {
                                saw_new_file = true;
                            }
                        }

                        // Only consider access events for files that we haven't seen
                        // before.
                        //
                        // This is important on FUSE filesystems, where we may not get a
                        // Create event. In other cases we're about to access the file, so
                        // we don't want an infinite loop where processing an Access event
                        // creates another Access event.
                        if matches!(event.kind, EventKind::Access(_)) && !saw_new_file {
                            continue;
                        }

                        let rename_mode = match event.kind {
                            EventKind::Modify(ModifyKind::Name(rename_mode)) => Some(rename_mode),
                            _ => None,
                        };

                        let files = abs_paths
                            .into_iter()
                            .filter_map(|(idx, path)| {
                                self.file_change(rename_mode, idx, &event.kind, path)
                            })
                            .collect();
                        self.send(loader::Message::Changed { files });
                    }
                }
            }
        }
    }

    fn load_entry(
        mut watch: impl FnMut(&Path),
        entry: loader::Entry,
        do_watch: bool,
        send_message: impl Fn(AbsPathBuf),
    ) -> Vec<(AbsPathBuf, Option<Vec<u8>>)> {
        match entry {
            loader::Entry::Files(files) => files
                .into_iter()
                .map(|file| {
                    if do_watch {
                        watch(file.as_ref());
                    }
                    let contents = read(file.as_path());
                    (file, contents)
                })
                .collect::<Vec<_>>(),
            loader::Entry::Directories(dirs) => {
                let mut res = Vec::new();

                for root in &dirs.include {
                    send_message(root.clone());
                    let walkdir =
                        WalkDir::new(root).follow_links(true).into_iter().filter_entry(|entry| {
                            if !entry.file_type().is_dir() {
                                return true;
                            }
                            let path = entry.path();

                            if path_might_be_cyclic(path) {
                                return false;
                            }

                            // We want to filter out subdirectories that are roots themselves, because they will be visited separately.
                            dirs.exclude.iter().all(|it| it != path)
                                && (root == path || dirs.include.iter().all(|it| it != path))
                        });

                    let files = walkdir.filter_map(|it| it.ok()).filter_map(|entry| {
                        let depth = entry.depth();
                        let is_dir = entry.file_type().is_dir();
                        let is_file = entry.file_type().is_file();
                        let is_symlink = entry.path_is_symlink();
                        let abs_path = AbsPathBuf::try_from(
                            Utf8PathBuf::from_path_buf(entry.into_path()).ok()?,
                        )
                        .ok()?;
                        if depth < 2 && is_dir {
                            send_message(abs_path.clone());
                        }
                        // Recursively watch each include root (depth = 0). Also watch any symlinked
                        // directories because macOS fsevent does not follow symlinks on its own.
                        if is_dir && do_watch && (depth == 0 || is_symlink) {
                            watch(abs_path.as_ref());
                        }
                        if !is_file {
                            return None;
                        }
                        let ext = abs_path.extension().unwrap_or_default();
                        if dirs.extensions.iter().all(|it| it.as_str() != ext) {
                            return None;
                        }
                        Some(abs_path)
                    });

                    res.extend(files.map(|file| {
                        let contents = read(file.as_path());
                        (file, contents)
                    }));
                }
                res
            }
        }
    }

    /// Determines the VFS update for the `idx`-th path of a watcher event.
    ///
    /// The old path of a rename no longer exists, so it is always reported as
    /// deleted. This matters on case-insensitive filesystems, where the old
    /// path of a case-only rename (e.g. `Test1.rs` -> `test1.rs`) still reads
    /// successfully, since it refers to the same file. Reporting its contents
    /// would resurrect a stale VFS entry under a path that no longer matches
    /// any directory entry.
    fn file_change(
        &mut self,
        rename_mode: Option<RenameMode>,
        idx: usize,
        kind: &EventKind,
        path: AbsPathBuf,
    ) -> Option<(AbsPathBuf, Option<Vec<u8>>)> {
        // Ignore events for files/directories that we're not watching.
        if !(self.watched_file_entries.contains(&path)
            || self.watched_dir_entries.iter().any(|dir| dir.contains_file(&path)))
        {
            return None;
        }

        // For removed files, fs::metadata() will return Err, but
        // we still want to update the VFS.
        if matches!(kind, EventKind::Remove(_)) {
            return Some((path, None));
        }

        // The first path of a `Both` rename event (and the only path of a
        // `From` event) is the old name, which is gone after the rename.
        match rename_mode {
            Some(RenameMode::From) => return Some((path, None)),
            Some(RenameMode::Both) if idx == 0 => return Some((path, None)),
            _ => {}
        }

        let meta = fs::metadata(&path).ok()?;
        if meta.file_type().is_dir()
            && self.watched_dir_entries.iter().any(|dir| dir.contains_dir(&path))
        {
            self.watch(path.as_ref());
            return None;
        }

        if !meta.file_type().is_file() {
            return None;
        }

        let contents = read_exactly_named(&path);
        Some((path, contents))
    }

    fn watch(&mut self, path: &Path) {
        if let Some((watcher, _)) = &mut self.watcher {
            log_notify_error(watcher.watch(path, RecursiveMode::Recursive));
        }
    }

    #[track_caller]
    fn send(&self, msg: loader::Message) {
        self.sender.send(msg).unwrap();
    }
}

fn read(path: &AbsPath) -> Option<Vec<u8>> {
    std::fs::read(path).ok()
}

/// Like [`read`], but additionally requires the file to exist under exactly
/// this name, see [`file_change`].
///
/// On case-insensitive filesystems, reading a path whose name only differs in
/// case from an existing file (e.g. `Test1.rs` vs `test1.rs`) succeeds. VFS
/// paths are compared case-sensitively, so reporting such a read as a file
/// under the requested name would create a VFS entry for a path that matches
/// no directory entry, and the entry could never be cleaned up by re-reading
/// the disk. Treat such paths as missing instead.
fn read_exactly_named(path: &AbsPath) -> Option<Vec<u8>> {
    let contents = read(path)?;

    let Some(file_name) = path.file_name() else { return Some(contents) };
    let Some(parent) = path.parent() else { return Some(contents) };
    let Ok(entries) = fs::read_dir(parent) else { return Some(contents) };

    let exists_under_this_name =
        entries.filter_map(Result::ok).any(|entry| entry.file_name() == file_name);
    exists_under_this_name.then_some(contents)
}

fn log_notify_error<T>(res: notify::Result<T>) -> Option<T> {
    res.map_err(|err| tracing::warn!("notify error: {}", err)).ok()
}

/// Is `path` a symlink to a parent directory?
///
/// Including this path is guaranteed to cause an infinite loop. This
/// heuristic is not sufficient to catch all symlink cycles (it's
/// possible to construct cycle using two or more symlinks), but it
/// catches common cases.
fn path_might_be_cyclic(path: &Path) -> bool {
    let Ok(destination) = std::fs::read_link(path) else {
        return false;
    };

    // If the symlink is of the form "../..", it's a parent symlink.
    let is_relative_parent =
        destination.components().all(|c| matches!(c, Component::CurDir | Component::ParentDir));

    is_relative_parent || path.starts_with(destination)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use notify::event::{ModifyKind, RenameMode};
    use paths::AbsPathBuf;
    use vfs::loader::Directories;

    use super::*;

    /// A temporary directory with a watched `foo.rs` file, for driving
    /// [`NotifyActor::file_change`] directly.
    struct TempDir {
        path: AbsPathBuf,
    }

    impl TempDir {
        fn new(test_name: &str) -> TempDir {
            let path = std::env::temp_dir().join(format!(
                "rust-analyzer-vfs-notify-{}-{}",
                std::process::id(),
                test_name,
            ));
            _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            let path = Utf8PathBuf::from_path_buf(path).unwrap();
            let path = AbsPathBuf::try_from(path).unwrap();
            TempDir { path }
        }

        fn write(&self, file_name: &str, contents: &str) -> AbsPathBuf {
            let path = self.path.join(file_name);
            fs::write(&path, contents).unwrap();
            path
        }

        /// An actor watching the whole temporary directory for `rs` files.
        fn actor(&self) -> NotifyActor {
            let (sender, _) = crossbeam_channel::unbounded();
            let mut actor = NotifyActor::new(sender);
            actor.watched_dir_entries.push(Directories {
                extensions: vec!["rs".to_owned()],
                include: vec![self.path.clone()],
                exclude: Vec::new(),
            });
            actor
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            _ = fs::remove_dir_all(&self.path);
        }
    }

    /// The old path of a rename event is reported as deleted, even if it is
    /// still readable. On case-insensitive filesystems this is what happens
    /// after a case-only rename like `Test1.rs` -> `test1.rs`, and reporting
    /// the old path's contents would resurrect a stale VFS entry.
    #[test]
    fn rename_event_old_path_is_reported_as_deleted() {
        let tmp = TempDir::new("rename_event_old_path_is_reported_as_deleted");
        let old_path = tmp.write("foo.rs", "fn foo() {}");
        let mut actor = tmp.actor();

        let change =
            actor.file_change(Some(RenameMode::Both), 0, &rename_event_kind(), old_path.clone());
        assert_eq!(change, Some((old_path.clone(), None)));

        let change =
            actor.file_change(Some(RenameMode::From), 0, &rename_event_kind(), old_path.clone());
        assert_eq!(change, Some((old_path, None)));
    }

    /// The new path of a rename event is read as usual.
    #[test]
    fn rename_event_new_path_is_read() {
        let tmp = TempDir::new("rename_event_new_path_is_read");
        let new_path = tmp.write("bar.rs", "fn bar() {}");
        let mut actor = tmp.actor();

        let change =
            actor.file_change(Some(RenameMode::Both), 1, &rename_event_kind(), new_path.clone());
        assert_eq!(change, Some((new_path, Some(b"fn bar() {}".to_vec()))));
    }

    /// Files are only picked up under the exact name they are stored under,
    /// even on case-insensitive filesystems, where reading e.g. `Foo.rs`
    /// succeeds while the file is actually named `foo.rs`.
    #[test]
    fn reads_require_the_exact_file_name() {
        let tmp = TempDir::new("reads_require_the_exact_file_name");
        let exact = tmp.write("foo.rs", "fn foo() {}");
        let case_flipped = tmp.path.join("Foo.rs");

        // On case-sensitive filesystems reading the flipped name fails
        // outright, on case-insensitive ones it succeeds; either way it must
        // not be reported as an existing file.
        assert!(read_exactly_named(&case_flipped).is_none());
        assert_eq!(read_exactly_named(&exact), Some(b"fn foo() {}".to_vec()));
    }

    fn rename_event_kind() -> EventKind {
        EventKind::Modify(ModifyKind::Name(RenameMode::Both))
    }
}
