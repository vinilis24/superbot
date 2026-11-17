//! Runs Bots: one task at a time per Bot, in order. Builds the system prompt
//! (identity, rules, memory), streams harness events to the web app, and saves replies.

use crate::adapters::{self, AbortReason, AgentEvent, RunInput};
use crate::db::{Bot, BotPatch, Db, Message};
use crate::screen::{self, Screens};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, watch};

/// Context guard: these files are injected into every run's prompt, so a runaway
/// memory file can't silently eat the context window.
const MEMORY_PROMPT_CAP: usize = 32 * 1024;
const USER_PROMPT_CAP: usize = 8 * 1024;

pub struct Config {
    pub data_dir: PathBuf,
    pub workspace: PathBuf,
    pub user_name: String,
    /// Added to PATH so Bots can run `botctl`.
    pub bin_dir: PathBuf,
    pub api_url: String,
    pub bot_token: String,
    pub run_timeout: Duration,
    pub default_harness: String,
}

struct Run {
    abort: watch::Sender<AbortReason>,
    status: String,
    draft: String,
}

#[derive(Default)]
struct BotState {
    queue: VecDeque<Message>,
    draining: bool,
    run: Option<Run>,
}

pub struct App {
    pub db: Db,
    pub cfg: Config,
    /// Every live update for the web app, as JSON text.
    pub tx: broadcast::Sender<String>,
    /// Each Bot's screen. None when this server has no display tools.
    pub screens: Option<Screens>,
    bots: Mutex<HashMap<String, BotState>>,
    /// Bots waiting on `botctl ask` or `botctl handoff`, by the message's id.
    waiters: Mutex<HashMap<i64, oneshot::Sender<String>>>,
    closing: AtomicBool,
}

impl App {
    pub fn new(db: Db, cfg: Config, screens: Option<Screens>) -> Arc<App> {
        Arc::new(App { db, cfg, tx: broadcast::channel(1024).0, screens, bots: Mutex::default(), waiters: Mutex::default(), closing: AtomicBool::new(false) })
    }

    pub fn is_running(&self, bot_id: &str) -> bool {
        self.state().get(bot_id).is_some_and(|s| s.run.is_some())
    }

    pub fn send(&self, v: Value) {
        let _ = self.tx.send(v.to_string());
    }

    fn state(&self) -> std::sync::MutexGuard<'_, HashMap<String, BotState>> {
        self.bots.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// What the web app sees for a Bot.
    pub fn bot_view(&self, b: &Bot) -> Value {
        let preview = self.db.last_message(&b.id).map(|m| {
            let t: String = m.text.chars().filter(|c| !"#*_`>[]".contains(*c)).collect();
            let t = adapters::short(&t);
            if t.is_empty() { m.question.and_then(|q| q["title"].as_str().map(String::from)).unwrap_or_default() } else { t }
        });
        let mut v = json!({
            "id": b.id, "name": b.name, "color": b.color, "shape": b.shape, "harness": b.harness, "model": b.model, "effort": b.effort,
            "primary": b.primary, "hidden": b.hidden, "unread": b.unread, "preview": preview.unwrap_or_default(), "working": false,
        });
        if let Some(run) = self.state().get(&b.id).and_then(|s| s.run.as_ref()) {
            v["working"] = json!(true);
            v["status"] = json!(run.status);
            v["draft"] = json!(run.draft);
        }
        v
    }

    pub fn push_bot(&self, id: &str) {
        if let Some(b) = self.db.bot(id) {
            self.send(json!({ "type": "bot", "bot": self.bot_view(&b) }));
        }
    }

    pub fn push_message(&self, m: &Message) {
        let bot = self.db.bot(&m.bot_id).map(|b| self.bot_view(&b));
        self.send(json!({ "type": "message", "message": m, "bot": bot }));
    }

    pub fn bot_dir(&self, id: &str) -> PathBuf {
        self.cfg.data_dir.join("bots").join(id)
    }
    fn memory_file(&self, id: &str) -> PathBuf {
        self.bot_dir(id).join("MEMORY.md")
    }
    fn user_file(&self) -> PathBuf {
        self.cfg.data_dir.join("USER.md")
    }

    pub fn init_bot_files(&self, bot: &Bot) {
        let _ = std::fs::create_dir_all(self.bot_dir(&bot.id));
        let mem = self.memory_file(&bot.id);
        if !mem.exists() {
            let _ = std::fs::write(&mem, format!("# {}\n\n## Job\n\nNot set yet. Ask the user.\n\n## Rules\n\n## Work log\n", bot.name));
        }
        let user = self.user_file();
        if !user.exists() {
            let _ = std::fs::write(&user, format!("# About the user\n\n- Name: {}\n", self.cfg.user_name));
        }
    }

    pub fn enqueue(self: &Arc<Self>, msg: Message) {
        let bot_id = msg.bot_id.clone();
        let mut st = self.state();
        let s = st.entry(bot_id.clone()).or_default();
        s.queue.push_back(msg);
        if !s.draining {
            s.draining = true;
            tokio::spawn(self.clone().drain(bot_id));
        }
    }

    async fn drain(self: Arc<Self>, bot_id: String) {
        loop {
            if self.closing.load(Ordering::SeqCst) {
                return; // left as is: the next start picks it up
            }
            // Pop and "queue is empty, stop draining" happen under one lock, so nothing is dropped.
            let next = {
                let mut st = self.state();
                let m = st.get_mut(&bot_id).and_then(|s| s.queue.pop_front());
                if m.is_none() {
                    st.remove(&bot_id);
                }
                m
            };
            let Some(msg) = next else { break };
            let before = self.db.last_id();
            if let Some(bot) = self.db.bot(&bot_id) {
                self.run_one(bot, &msg).await;
            }
            if self.closing.load(Ordering::SeqCst) {
                return; // left "running": the next start reports it
            }
            self.db.set_status(msg.id, "done");
            if msg.role == "peer" {
                self.answer_peer(&msg, before);
            }
        }
        self.send(json!({ "type": "status", "botId": bot_id, "working": false }));
    }

    async fn run_one(self: &Arc<Self>, mut bot: Bot, msg: &Message) {
        if adapters::get(&bot.harness).is_none() {
            return self.say(&bot.id, &format!("Unknown harness \"{}\".", bot.harness), true);
        }
        // A saved model may have been removed from the harness since it was picked.
        // Verify before spawning, so it can't run as a different model silently.
        if let Some(model) = bot.model.as_deref() {
            let harness = bot.harness.clone();
            let list = tokio::task::spawn_blocking(move || adapters::models(&harness)).await.unwrap_or_default();
            if let Some(msg) = model_problem(&bot.harness, Some(model), &list) {
                return self.say(&bot.id, &format!("{msg} Pick another model in this Bot's settings."), true);
            }
        }
        self.init_bot_files(&bot);
        self.db.set_status(msg.id, "running");
        let from = || msg.from_bot.as_deref().and_then(|id| self.db.bot(id)).map(|b| format!("{} (id {})", b.name, b.id)).unwrap_or("another Bot".into());
        let prompt = match msg.role.as_str() {
            "routine" => format!("(Scheduled routine) {}", msg.text),
            "peer" => format!("(Message from {}, another Bot. Your reply goes back to them, not the user.)\n\n{}", from(), msg.text),
            "reply" => format!("(Reply from {} to what you asked earlier. Tell the user what matters.)\n\n{}", from(), msg.text),
            _ => msg.text.clone(),
        };

        for attempt in 0..2 {
            let (abort_tx, abort_rx) = watch::channel(None);
            self.state().entry(bot.id.clone()).or_default().run =
                Some(Run { abort: abort_tx.clone(), status: "Thinking".into(), draft: String::new() });
            self.send(json!({ "type": "status", "botId": bot.id, "working": true, "status": "Thinking" }));
            let timeout = self.cfg.run_timeout;
            let timer = tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                abort_tx.send_if_modified(|r| r.get_or_insert("timeout") == &"timeout");
            });

            let mut env = HashMap::from([
                ("BOT_ID".to_string(), bot.id.clone()),
                ("BOT_API".to_string(), self.cfg.api_url.clone()),
                ("BOT_TOKEN".to_string(), self.cfg.bot_token.clone()),
            ]);
            let path = std::env::var("PATH").unwrap_or_default();
            env.insert("PATH".into(), format!("{}:{path}", self.cfg.bin_dir.display()));
            if let Some(screens) = &self.screens
                && let Some(info) = screens.ensure(&bot.id, |id| self.is_running(id)).await
            {
                env.extend(screen::bot_env(&info, &bot.id, &self.bot_dir(&bot.id)));
            }
            let input = RunInput {
                prompt: prompt.clone(),
                system: self.system_prompt(&bot),
                session_id: bot.session_id.clone(),
                cwd: self.cfg.workspace.clone(),
                model: bot.model.clone(),
                effort: bot.effort.clone(),
                env,
                state_dir: self.bot_dir(&bot.id),
            };

            let mut errors: Vec<String> = Vec::new();
            // Text already shown as normal bubbles, so a failure the harness also
            // printed as chat text isn't reported twice below.
            let mut said: Vec<String> = Vec::new();
            let mut replies = 0;
            let id = bot.id.clone();
            adapters::run(adapters::get(&bot.harness).unwrap(), input, abort_rx.clone(), |e| match e {
                AgentEvent::Session(s) => {
                    if self.db.bot(&id).and_then(|b| b.session_id).as_deref() != Some(&s) {
                        self.db.update_bot(&id, BotPatch { session_id: Some(Some(s)), ..Default::default() });
                    }
                }
                AgentEvent::TextDelta(t) => {
                    let draft = self.with_run(&id, |r| { r.draft.push_str(&t); r.draft.clone() });
                    self.send(json!({ "type": "draft", "botId": id, "text": draft }));
                }
                AgentEvent::Text(t) => {
                    self.with_run(&id, |r| r.draft.clear());
                    replies += 1;
                    said.push(t.clone());
                    self.say(&id, &t, false);
                }
                AgentEvent::Tool { name, detail } => {
                    let status = tool_label(&name, &detail);
                    self.with_run(&id, |r| r.status = status.clone());
                    self.send(json!({ "type": "status", "botId": id, "working": true, "status": status }));
                }
                AgentEvent::Error(m) => errors.push(m),
            })
            .await;
            timer.abort();
            // Done running. The stream may have ended without a final block: keep what was streamed.
            let draft = self.state().get_mut(&id).and_then(|s| s.run.take()).map(|r| r.draft).unwrap_or_default();
            if !draft.trim().is_empty() {
                replies += 1;
                said.push(draft.clone());
                self.say(&id, &draft, false);
            }

            // The harness lost its session (e.g. its files were wiped): start fresh once.
            let lost = errors.iter().any(|m| {
                let m = m.to_lowercase();
                m.contains("no conversation found")
                    || ((m.contains("session") || m.contains("conversation"))
                        && (m.contains("not found") || m.contains("no such") || m.contains("does not exist")))
            });
            if lost && bot.session_id.is_some() && replies == 0 && attempt == 0 {
                self.db.update_bot(&id, BotPatch { session_id: Some(None), ..Default::default() });
                bot.session_id = None;
                continue;
            }

            match *abort_rx.borrow() {
                Some("shutdown") => {}
                Some("stopped") => self.say(&id, "Stopped.", true),
                Some("timeout") => {
                    let mins = self.cfg.run_timeout.as_secs() / 60;
                    self.say(&id, &format!("I hit the {mins} minute limit for one task and stopped."), true)
                }
                _ if !errors.is_empty() => {
                    // Some harnesses print the failure as chat text and also exit
                    // non-zero, which would show the same error twice.
                    if !errors_already_said(&errors, &said) {
                        self.say(&id, &errors.join("\n\n"), true);
                    }
                }
                _ if replies == 0 => self.say(&id, "Done.", false),
                _ => {}
            }
            return;
        }
    }

    fn with_run<T: Default>(&self, bot_id: &str, f: impl FnOnce(&mut Run) -> T) -> T {
        self.state().get_mut(bot_id).and_then(|s| s.run.as_mut()).map(f).unwrap_or_default()
    }

    /// Another Bot asks this one something. Resolves with the reply, or None if it
    /// takes longer than `wait`; then the reply is delivered to the asker later.
    pub async fn ask(self: &Arc<Self>, from: &Bot, to: &Bot, text: &str, wait: Duration) -> Option<String> {
        let msg = self.db.add_message_from(&to.id, "peer", text, None, false, Some("queued"), Some(&from.id));
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().unwrap().insert(msg.id, tx);
        self.push_message(&msg);
        self.enqueue(msg.clone());
        let reply = tokio::time::timeout(wait, rx).await.ok().and_then(Result::ok);
        if reply.is_none() {
            self.waiters.lock().unwrap().remove(&msg.id);
        }
        reply
    }

    /// The Bot needs the user on its screen (a login, 2FA, a CAPTCHA). Posts a card
    /// with "Done" and waits for the answer, or None after `wait`; then the answer
    /// reaches the Bot later as a normal message.
    pub async fn handoff(&self, bot: &Bot, text: &str, wait: Duration) -> Option<String> {
        let q = json!({ "title": text, "options": ["Done", "I can't right now"], "handoff": true });
        let m = self.db.add_message(&bot.id, "bot", "", Some(&q), false, None);
        self.db.update_bot(&bot.id, BotPatch { unread: Some(true), ..Default::default() });
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().unwrap().insert(m.id, tx);
        self.push_message(&m);
        let answer = tokio::time::timeout(wait, rx).await.ok().and_then(Result::ok);
        if answer.is_none() {
            self.waiters.lock().unwrap().remove(&m.id);
        }
        answer
    }

    /// Takes the waiter for a handoff card, if its Bot is still waiting.
    pub fn take_handoff(&self, message_id: i64) -> Option<oneshot::Sender<String>> {
        let is_handoff = self.db.message(message_id).and_then(|m| m.question).is_some_and(|q| q["handoff"] == json!(true));
        if is_handoff { self.waiters.lock().unwrap().remove(&message_id) } else { None }
    }

    /// Sends the asked Bot's reply to whoever asked: the waiting `botctl ask`,
    /// or, if it gave up waiting, as a new message in the asker's queue.
    fn answer_peer(self: &Arc<Self>, msg: &Message, before: i64) {
        let texts: Vec<String> = self.db.replies_after(&msg.bot_id, before).into_iter().map(|m| m.text).collect();
        let reply = if texts.is_empty() { "(No reply.)".to_string() } else { texts.join("\n\n") };
        let waiter = self.waiters.lock().unwrap().remove(&msg.id);
        let unsent = match waiter {
            Some(tx) => tx.send(reply).err(),
            None => Some(reply),
        };
        if let Some(reply) = unsent {
            let Some(asker) = msg.from_bot.as_deref().filter(|id| self.db.bot(id).is_some()) else { return };
            let m = self.db.add_message_from(asker, "reply", &reply, None, false, Some("queued"), Some(&msg.bot_id));
            self.push_message(&m);
            self.enqueue(m);
        }
    }

    /// Saves one Bot chat bubble. A trailing ```question block becomes a choice card.
    pub fn say(&self, bot_id: &str, text: &str, error: bool) {
        let (text, question) = split_question(text);
        let m = self.db.add_message(bot_id, "bot", text.trim(), question.as_ref(), error, None);
        self.db.update_bot(bot_id, BotPatch { unread: Some(true), ..Default::default() });
        self.push_message(&m);
    }

    /// Stops the current task and drops queued ones.
    pub fn stop(&self, bot_id: &str) {
        let mut st = self.state();
        if let Some(s) = st.get_mut(bot_id) {
            for m in s.queue.drain(..) {
                self.db.set_status(m.id, "done");
            }
            if let Some(r) = &s.run {
                r.abort.send_if_modified(|v| v.get_or_insert("stopped") == &"stopped");
            }
        }
    }

    /// Kills running agents. Queued messages run after the restart.
    pub fn shutdown(&self) {
        self.closing.store(true, Ordering::SeqCst);
        for s in self.state().values() {
            if let Some(r) = &s.run {
                r.abort.send_if_modified(|v| v.get_or_insert("shutdown") == &"shutdown");
            }
        }
    }

    pub fn is_idle(&self) -> bool {
        self.state().values().all(|s| s.run.is_none())
    }

    /// After a restart: queued messages run. Interrupted ones are reported, not
    /// re-run, because re-running could repeat actions like sending an email.
    pub fn recover(self: &Arc<Self>) {
        for m in self.recover_pending() {
            self.enqueue(m);
        }
    }

    /// Marks interrupted messages and returns the ones still worth running.
    /// Split from `recover` so the decision stays testable without a runtime.
    fn recover_pending(&self) -> Vec<Message> {
        let mut run = Vec::new();
        for m in self.db.pending() {
            if self.db.bot(&m.bot_id).is_none() {
                continue;
            }
            if m.status.as_deref() == Some("running") {
                self.db.set_status(m.id, "done");
                self.say(&m.bot_id, "A server restart interrupted me before I finished. Send it again if you still need it.", true);
            } else {
                run.push(m);
            }
        }
        run
    }

    fn system_prompt(&self, bot: &Bot) -> String {
        let read = |p: PathBuf, cap: usize| {
            let (s, cut) = read_capped(p, cap);
            if cut {
                format!("{s}\n\n(This file got too long and was cut. Keep it short: remove what no longer matters.)")
            } else {
                s
            }
        };
        let others: Vec<String> =
            self.db.bots().iter().filter(|b| b.id != bot.id).map(|b| format!("- {} (id {})", b.name, b.id)).collect();
        let primary = if bot.primary {
            " You are their primary Bot: their main point of contact. You help them decide which Bot should own which job."
        } else {
            ""
        };
        format!(
            r#"You are {name}, an AI teammate of {user}.{primary}
They talk to you in a chat app, like messaging a coworker. Finish work end to end.

## Your computer
- You run on a Linux computer shared with the user's other Bots. Your working folder is {ws}.
- Files, browser sign-ins, and credentials on it are shared by all Bots. Keep durable work in clear project folders.

## Memory
- Your memory file: {mem_path}. It holds your job, the user's rules and preferences for you, and a short dated log of important work.
- Shared facts about the user, used by every Bot: {user_path}. Name, time zone, and similar.
- Update these files yourself when you learn something lasting, without asking. Keep them short. Never store passwords, keys, or card numbers.
- Current contents:
<memory>
{mem}
</memory>
<user>
{user_facts}
</user>

## Routines
- For recurring or scheduled work, use `{botctl}`. Run `{botctl} help` to see how.
{screen}
## Style
- Reply like a teammate in chat: short, plain English. Markdown is fine.
- Each separate text block you write is shown as its own chat bubble. Before long work, send a one-line heads-up.
- Ask before anything you can't undo: sending messages, publishing, deleting, paying.
- To ask a multiple-choice question, end your reply with this block, and nothing after it:
```question
{{"title": "Your question?", "options": ["First", "Second", "Third"]}}
```

## Other Bots
{others}
- To ask another Bot something or hand it a task, run: {botctl} ask "<Bot name or id>" "<message>". It prints their reply. It can take minutes, so give the command a long timeout (10 minutes). If they take longer, their reply arrives later as a new message.
- Only loop in another Bot when the user asks or it clearly helps. Never ask the Bot that is asking you.

Current time: {now}."#,
            name = bot.name,
            // Full path: some harnesses run commands in a login shell that resets PATH.
            botctl = self.cfg.bin_dir.join("botctl").display(),
            screen = if self.screens.is_some() { screen_prompt(&self.cfg.bin_dir.join("botctl").display().to_string()) } else { String::new() },
            user = self.cfg.user_name,
            ws = self.cfg.workspace.display(),
            mem_path = self.memory_file(&bot.id).display(),
            user_path = self.user_file().display(),
            mem = read(self.memory_file(&bot.id), MEMORY_PROMPT_CAP),
            user_facts = read(self.user_file(), USER_PROMPT_CAP),
            others = if others.is_empty() { "None yet.".into() } else { others.join("\n") },
            now = chrono::Local::now().format("%A, %B %-d, %Y, %H:%M (UTC%:z)"),
        )
    }
}

/// Splits off a trailing ```question {json}``` block.
fn split_question(text: &str) -> (&str, Option<Value>) {
    let t = text.trim_end();
    let Some(start) = t.rfind("```question") else { return (text, None) };
    let Some(body) = t[start + 11..].strip_suffix("```") else { return (text, None) };
    let Ok(q) = serde_json::from_str::<Value>(body.trim()) else { return (text, None) };
    let (Some(title), Some(opts)) = (q["title"].as_str(), q["options"].as_array()) else { return (text, None) };
    let options: Vec<String> = opts.iter().take(6).map(|o| o.as_str().map(String::from).unwrap_or(o.to_string())).collect();
    (&t[..start], Some(json!({ "title": title, "options": options })))
}

/// True when the run's own chat text already carries every error, compared
/// loosely (case and whitespace) so a genuinely different error still shows.
fn errors_already_said(errors: &[String], said: &[String]) -> bool {
    let norm = |s: &str| s.to_lowercase().split_whitespace().collect::<Vec<_>>().join(" ");
    let said: Vec<String> = said.iter().map(|s| norm(s)).collect();
    !errors.is_empty() && errors.iter().all(|e| {
        let e = norm(e);
        !e.is_empty() && said.iter().any(|s| s.contains(&e))
    })
}

/// Why a Bot's model can't run on its harness, if it can't.
/// A Bot with no model uses the harness default: always fine. A model is valid
/// only as an exact id or alias from that harness's own list: a stale or
/// hand-typed name must never run as a different model silently.
pub(crate) fn model_problem(harness: &str, model: Option<&str>, list: &[adapters::ModelOption]) -> Option<String> {
    let model = model?;
    if list.is_empty() {
        return Some(format!("Cannot verify models for {harness}; its model list is empty. Try again later."));
    }
    let offered = list.iter().any(|o| o.id == model || o.aliases.iter().any(|a| a == model));
    (!offered).then(|| {
        let ids: Vec<&str> = list.iter().take(8).map(|o| o.id.as_str()).collect();
        let more = if list.len() > ids.len() { ", …" } else { "" };
        format!("Model \"{model}\" is not offered by {harness}. Choose one from its model list ({}{more}).", ids.join(", "))
    })
}

fn tool_label(name: &str, detail: &str) -> String {
    let n = name.to_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| n.contains(w));
    let verb = if has(&["bash", "shell", "command"]) {
        "Running".to_string()
    } else if has(&["read", "view"]) {
        "Reading".into()
    } else if has(&["write", "edit", "patch", "file_change"]) {
        "Editing".into()
    } else if has(&["search", "grep", "glob", "find"]) {
        "Searching".into()
    } else if has(&["web", "fetch", "browse"]) {
        "Browsing".into()
    } else {
        format!("Using {name}")
    };
    if detail.is_empty() { verb } else { format!("{verb}: {detail}") }
}

/// How to use the Bot's screen. Tools are plain CLIs, so this works on every harness.
fn screen_prompt(botctl: &str) -> String {
    format!(
        r#"
## Your screen
- You have your own screen with a browser. The user can watch it live and take over.
- Prefer APIs and command-line tools. Use the browser for sites that have no API.
- Websites: `agent-browser`. Before your first use in a task, run `agent-browser skills get core` and follow it, with one exception: your session and browser profile are already set for you, so never pass `--session`, `--profile`, or `--headed`, and never create other sessions. Your sign-ins persist between tasks.
- Desktop apps: `agent-desktop`. Before your first use, run `agent-desktop skills get desktop`.
- When you need the user on your screen (a login, 2FA code, CAPTCHA, or payment check), run: {botctl} handoff "<what you need, in one line>". It waits until they finish, so give the command a 10 minute timeout. Never ask for passwords in chat.
"#
    )
}

/// Reads a file cut to `cap` bytes at a char boundary. Returns the text and
/// whether it was cut.
fn read_capped(path: PathBuf, cap: usize) -> (String, bool) {
    let mut buf = Vec::new();
    if let Ok(f) = std::fs::File::open(&path) {
        use std::io::Read;
        let _ = f.take(cap as u64 + 1).read_to_end(&mut buf);
    }
    if buf.len() <= cap {
        return (String::from_utf8_lossy(&buf).trim().to_string(), false);
    }
    // Read one byte past `cap`, so buf[cap] exists as the sentinel for the
    // char-boundary check: the prefix is valid when buf[end] starts a char.
    let mut end = cap;
    while end > 0 && (buf[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    buf.truncate(end);
    (String::from_utf8_lossy(&buf).trim().to_string(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_cap() {
        let dir = std::env::temp_dir().join(format!("botd-cap-{}", crate::short_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("MEMORY.md");

        std::fs::write(&p, "short note").unwrap();
        assert_eq!(read_capped(p.clone(), 100), ("short note".to_string(), false));

        std::fs::write(&p, "y".repeat(100)).unwrap();
        let (s, cut) = read_capped(p.clone(), 100);
        assert!(!cut && s.len() == 100);

        std::fs::write(&p, "x".repeat(500)).unwrap();
        let (s, cut) = read_capped(p.clone(), 100);
        assert!(cut && s.len() == 100);

        // A cut inside a multi-byte char stays valid UTF-8.
        std::fs::write(&p, "é".repeat(100)).unwrap();
        let (s, cut) = read_capped(p, 100);
        assert!(cut && !s.contains('\u{FFFD}'));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Isolated App with a temp data dir and no screen tools.
    fn test_app() -> (Arc<App>, PathBuf) {
        let dir = std::env::temp_dir().join(format!("botd-runner-{}", crate::short_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Db::open(&dir.join("botd.db")).unwrap();
        let cfg = Config {
            data_dir: dir.join("data"),
            workspace: dir.join("ws"),
            user_name: "Tester".into(),
            bin_dir: dir.join("bin"),
            api_url: "http://127.0.0.1:1".into(),
            bot_token: "test-token".into(),
            run_timeout: Duration::from_secs(60),
            default_harness: "claude".into(),
        };
        (App::new(db, cfg, None), dir)
    }

    #[test]
    fn split_question_takes_trailing_block() {
        let text = "Ready.\n```question\n{\"title\":\"Which one?\",\"options\":[\"First\",\"Second\"]}\n```";
        let (body, q) = split_question(text);
        assert_eq!(body.trim(), "Ready.");
        let q = q.unwrap();
        assert_eq!(q["title"], json!("Which one?"));
        assert_eq!(q["options"], json!(["First", "Second"]));
    }

    #[test]
    fn split_question_keeps_text_on_bad_or_missing_block() {
        // Invalid JSON: the whole text stays as-is.
        let bad = "Hello\n```question\n{not json}\n```";
        assert_eq!(split_question(bad), (bad, None));

        // No block at all.
        let plain = "Just a normal reply.";
        assert_eq!(split_question(plain), (plain, None));

        // Missing closing fence.
        let open = "Hello\n```question\n{\"title\":\"Q\",\"options\":[\"A\"]}";
        assert_eq!(split_question(open), (open, None));

        // Valid JSON, but no title/options.
        let incomplete = "Hello\n```question\n{\"title\":\"Q\"}\n```";
        assert_eq!(split_question(incomplete), (incomplete, None));
    }

    #[test]
    fn split_question_caps_options_at_six() {
        let opts: Vec<String> = (1..=8).map(|i| format!("\"Option {i}\"")).collect();
        let text = format!("Pick.\n```question\n{{\"title\":\"Pick\",\"options\":[{}]}}\n```", opts.join(","));
        let (body, q) = split_question(&text);
        assert_eq!(body.trim(), "Pick.");
        let options = q.unwrap()["options"].as_array().unwrap().clone();
        assert_eq!(options.len(), 6);
        assert_eq!(options[5], json!("Option 6"));
    }

    #[test]
    fn errors_already_said_matches_loosely() {
        let errors = vec!["Not logged in · Please run /login".to_string()];
        assert!(errors_already_said(&errors, &["not logged in · please run /login".to_string()]));
        assert!(errors_already_said(&errors, &["Error: Not logged in · Please run /login\n".to_string()]));
        assert!(!errors_already_said(&errors, &["Something else".to_string()]));
        assert!(!errors_already_said(&errors, &[]));
        assert!(!errors_already_said(&[], &["Anything".to_string()]));
        // One error shown is not enough when a different one is new.
        let two = vec!["A failed".to_string(), "B failed".to_string()];
        assert!(!errors_already_said(&two, &["A failed".to_string()]));
        assert!(errors_already_said(&two, &["A failed and B failed".to_string()]));
    }

    #[test]
    fn stop_marks_queue_done_and_aborts_run() {
        let (app, dir) = test_app();
        let bot = app.db.create_bot("Runner", "#fff", "circle", "claude", None, false);
        let queued = app.db.add_message(&bot.id, "user", "hi", None, false, Some("queued"));
        let (tx, rx) = watch::channel::<AbortReason>(None);
        {
            let mut st = app.state();
            let s = st.entry(bot.id.clone()).or_default();
            s.queue.push_back(queued.clone());
            s.run = Some(Run { abort: tx, status: "Thinking".into(), draft: String::new() });
        }

        app.stop(&bot.id);

        assert_eq!(app.db.message(queued.id).unwrap().status.as_deref(), Some("done"));
        assert_eq!(*rx.borrow(), Some("stopped"));
        assert!(app.state().get(&bot.id).unwrap().queue.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn stop_with_unknown_bot_does_nothing() {
        let (app, dir) = test_app();
        app.stop("missing");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recover_marks_interrupted_message_done() {
        let (app, dir) = test_app();
        let bot = app.db.create_bot("Runner", "#fff", "circle", "claude", None, false);
        let interrupted = app.db.add_message(&bot.id, "user", "do work", None, false, Some("running"));

        let run = app.recover_pending();

        assert!(run.is_empty(), "an interrupted run is reported, not re-run");
        assert_eq!(app.db.message(interrupted.id).unwrap().status.as_deref(), Some("done"));
        let last = app.db.last_message(&bot.id).unwrap();
        assert!(last.error);
        assert!(last.text.contains("interrupted"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recover_requeues_queued_but_not_done() {
        let (app, dir) = test_app();
        let bot = app.db.create_bot("Runner", "#fff", "circle", "claude", None, false);
        let queued = app.db.add_message(&bot.id, "user", "later", None, false, Some("queued"));
        let done = app.db.add_message(&bot.id, "user", "earlier", None, false, Some("done"));

        let run = app.recover_pending();

        let ids: Vec<i64> = run.iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![queued.id], "only the queued message runs again");
        assert!(!ids.contains(&done.id));
        assert_eq!(app.db.message(queued.id).unwrap().status.as_deref(), Some("queued"));
        // The interrupted path adds an error bubble; the queued one does not.
        assert_eq!(app.db.messages(&bot.id, 10).len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recover_skips_messages_of_deleted_bots() {
        let (app, dir) = test_app();
        let bot = app.db.create_bot("Runner", "#fff", "circle", "claude", None, false);
        let orphan = app.db.add_message(&bot.id, "user", "orphan", None, false, Some("queued"));
        app.db.delete_bot(&bot.id);

        let run = app.recover_pending();

        assert!(run.is_empty());
        // Deleted bot cascaded the message away.
        assert!(app.db.message(orphan.id).is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn opt(id: &str, aliases: &[&str]) -> adapters::ModelOption {
        adapters::ModelOption {
            id: id.into(),
            label: id.into(),
            efforts: Vec::new(),
            aliases: aliases.iter().map(|a| a.to_string()).collect(),
        }
    }

    #[test]
    fn model_unset_is_always_fine() {
        let list = vec![opt("sonnet", &[])];
        assert_eq!(model_problem("claude", None, &list), None);
        // Even an empty (unverifiable) list is fine with no model set.
        assert_eq!(model_problem("claude", None, &[]), None);
    }

    #[test]
    fn model_accepts_exact_id_and_alias() {
        let list = vec![opt("claude-sonnet-4", &["sonnet"]), opt("claude-opus-4", &["opus"])];
        assert_eq!(model_problem("claude", Some("claude-opus-4"), &list), None);
        assert_eq!(model_problem("claude", Some("opus"), &list), None);
    }

    #[test]
    fn model_rejects_unknown_name() {
        let list = vec![opt("claude-sonnet-4", &["sonnet"])];
        let msg = model_problem("claude", Some("gpt-5"), &list).unwrap();
        assert!(msg.contains("gpt-5") && msg.contains("claude"));
        assert!(msg.contains("claude-sonnet-4"), "message lists the choices");
    }

    #[test]
    fn model_rejects_when_list_is_empty() {
        // Never allow an unverifiable model.
        let msg = model_problem("codex", Some("gpt-5"), &[]).unwrap();
        assert!(msg.contains("Cannot verify") && msg.contains("codex"));
    }
}
