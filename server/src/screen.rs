//! Each Bot's screen: a virtual display (Xvfb) with a window manager (Openbox) and
//! its own D-Bus session (agent-desktop reads apps through it), streamed to the web
//! app by voxl. Started on first use, stopped when idle. Off when Xvfb isn't installed.
//! A screen is a work surface, not a security boundary: every Bot runs as one user.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

pub struct Screens {
    max: u16,
    idle: Duration,
    /// Display numbers are base+slot; voxl ports are 6800+base+slot (6901, 6902, ...).
    base: u16,
    size: String,
    run_dir: PathBuf,
    live: Mutex<HashMap<String, Screen>>,
}

struct Screen {
    slot: u16,
    token: String,
    used: Instant,
    procs: Vec<Child>,
}

/// What a Bot run and the web app need to reach a screen.
pub struct Info {
    pub display: String,
    pub bus: String,
    pub port: u16,
    pub token: String,
}

pub fn on_path(bin: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|p| std::env::split_paths(&p).any(|d| d.join(bin).is_file()))
}

fn spawn(cmd: &mut Command) -> std::io::Result<Child> {
    cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true).spawn()
}

/// Removes what an Xvfb leaves behind: its display lock and socket.
fn clean_display(n: u16) {
    let _ = std::fs::remove_file(format!("/tmp/.X{n}-lock"));
    let _ = std::fs::remove_file(format!("/tmp/.X11-unix/X{n}"));
}

/// Kills an Xvfb that still holds display `n`, so a new one can take the socket.
/// A crashed or half-stopped screen would otherwise leave the display busy.
async fn kill_stale_xvfb(n: u16) {
    let Some(pid) = std::fs::read_to_string(format!("/tmp/.X{n}-lock")).ok().and_then(|t| t.trim().parse::<i32>().ok()) else {
        return;
    };
    if pid <= 1 {
        return;
    }
    // The lock's pid may belong to something else: only kill a process that is Xvfb on this display.
    let Ok(cmd) = std::fs::read(format!("/proc/{pid}/cmdline")) else { return };
    let display = format!(":{n}");
    let args: Vec<String> = String::from_utf8_lossy(&cmd).split('\0').map(str::to_string).collect();
    if !args.iter().any(|a| a.contains("Xvfb")) || !args.contains(&display) {
        return;
    }
    unsafe { libc::kill(pid, libc::SIGTERM) };
    for _ in 0..20 {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    unsafe { libc::kill(pid, libc::SIGKILL) };
    for _ in 0..20 {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Kills and reaps a screen's processes, waiting at most `timeout` in total.
async fn kill_and_wait(procs: &mut [Child], timeout: Duration) {
    for c in procs.iter_mut() {
        let _ = c.start_kill();
    }
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_done = true;
        for c in procs.iter_mut() {
            if matches!(c.try_wait(), Ok(None)) {
                all_done = false;
            }
        }
        if all_done || Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The agent-browser session that belongs to a Bot.
pub fn browser_session(bot_id: &str) -> String {
    format!("bot-{bot_id}")
}

impl Screens {
    pub fn from_env(run_dir: PathBuf) -> Option<Screens> {
        let num = |k: &str, d: u64| crate::env(k, &d.to_string()).parse::<u64>().unwrap_or(d);
        if crate::env("SCREENS", "on") == "off" || !on_path("Xvfb") || !on_path("openbox") {
            return None;
        }
        std::fs::create_dir_all(&run_dir).ok()?;
        Some(Screens {
            max: num("MAX_SCREENS", 3).clamp(1, 50) as u16,
            idle: Duration::from_secs(60 * num("SCREEN_IDLE_MINUTES", 10)),
            base: num("SCREEN_BASE", 100).clamp(1, 1000) as u16,
            size: crate::env("SCREEN_SIZE", "1280x800"),
            run_dir,
            live: Mutex::default(),
        })
    }

    fn info(&self, s: &Screen) -> Info {
        let n = self.base + s.slot;
        Info {
            display: format!(":{n}"),
            bus: format!("unix:path={}", self.run_dir.join(format!("bus-{n}")).display()),
            port: 6800 + n,
            token: s.token.clone(),
        }
    }

    /// The Bot's screen, started if needed. When all are in use, the least recently
    /// used screen of a Bot that isn't working is stopped. None if none can be.
    pub async fn ensure(&self, bot_id: &str, busy: impl Fn(&str) -> bool) -> Option<Info> {
        let mut live = self.live.lock().await;
        if let Some(s) = live.get_mut(bot_id) {
            if s.procs.iter_mut().all(|c| matches!(c.try_wait(), Ok(None))) {
                s.used = Instant::now();
                return Some(self.info(s));
            }
            let s = live.remove(bot_id).unwrap(); // something crashed: start over
            stop(bot_id, s).await;
        }
        if live.len() >= self.max as usize {
            let victim = live.iter().filter(|(id, _)| !busy(id)).min_by_key(|(_, s)| s.used).map(|(id, _)| id.clone())?;
            let s = live.remove(&victim).unwrap();
            stop(&victim, s).await;
        }
        let slot = (1..=self.max).find(|n| live.values().all(|s| s.slot != *n))?;
        match self.start(slot).await {
            Ok(s) => {
                let info = self.info(&s);
                live.insert(bot_id.to_string(), s);
                Some(info)
            }
            Err(e) => {
                eprintln!("Screen {slot} failed to start: {e}");
                None
            }
        }
    }

    async fn start(&self, slot: u16) -> std::io::Result<Screen> {
        let token: String = crate::random_bytes(16).iter().map(|x| format!("{x:02x}")).collect();
        let mut s = Screen { slot, token, used: Instant::now(), procs: Vec::new() };
        let Info { display, bus, port, .. } = self.info(&s);
        let n = self.base + slot;

        // Leftovers from a crash would stop Xvfb from starting.
        let sock = PathBuf::from(format!("/tmp/.X11-unix/X{n}"));
        kill_stale_xvfb(n).await;
        clean_display(n);
        s.procs.push(spawn(Command::new("Xvfb").args([&display, "-screen", "0", &format!("{}x24", self.size), "-nolisten", "tcp"]))?);
        for _ in 0..50 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !sock.exists() {
            return Err(std::io::Error::other("Xvfb didn't open its display"));
        }

        let bus_path = self.run_dir.join(format!("bus-{n}"));
        let _ = std::fs::remove_file(&bus_path);
        s.procs.push(spawn(Command::new("dbus-daemon").args(["--session", "--nofork", &format!("--address={bus}")]))?);
        s.procs.push(spawn(Command::new("openbox").env("DISPLAY", &display).env("DBUS_SESSION_BUS_ADDRESS", &bus))?);
        if on_path("voxl") {
            // voxl's own token login guards the stream; the proxy in front adds HTTPS.
            s.procs.push(spawn(
                Command::new("voxl")
                    .args(["--addr", "127.0.0.1", "--port", &port.to_string(), "--allowed-hosts", "*"])
                    .args(["--audio-enabled", "false", "--file-transfers", "none", "--enable-clipboard", "true"])
                    .env("DISPLAY", &display)
                    .env("VOXL_PASSWORD", &s.token),
            )?);
        }
        Ok(s)
    }

    /// Stops screens that nobody used for a while.
    pub async fn reap(&self, busy: impl Fn(&str) -> bool) {
        let mut live = self.live.lock().await;
        let idle: Vec<String> =
            live.iter().filter(|(id, s)| !busy(id) && s.used.elapsed() > self.idle).map(|(id, _)| id.clone()).collect();
        for id in idle {
            let s = live.remove(&id).unwrap();
            stop(&id, s).await;
        }
    }

    pub async fn remove(&self, bot_id: &str) {
        let s = self.live.lock().await.remove(bot_id);
        if let Some(s) = s {
            stop(bot_id, s).await;
        }
    }

    /// Stops every screen on shutdown. `process::exit` skips destructors, so the signal
    /// handler must kill and reap the children itself; otherwise Xvfb, D-Bus, Openbox,
    /// and voxl survive as orphans holding displays and ports.
    pub async fn shutdown(&self) {
        let mut live = self.live.lock().await;
        for (_, mut s) in live.drain() {
            kill_and_wait(&mut s.procs, Duration::from_secs(3)).await;
            clean_display(self.base + s.slot);
        }
    }
}

/// Closes the Bot's browser cleanly (so its profile saves), then kills the screen.
async fn stop(bot_id: &str, mut s: Screen) {
    if on_path("agent-browser") {
        let close = Command::new("agent-browser").args(["--session", &browser_session(bot_id), "close"]).stdin(Stdio::null()).output();
        let _ = tokio::time::timeout(Duration::from_secs(10), close).await;
    }
    // Wait for the children instead of dropping them: a still-running Xvfb would keep
    // the display and its socket, and the next screen on this slot would fail to start.
    kill_and_wait(&mut s.procs, Duration::from_secs(3)).await;
}

/// Environment for a Bot's harness, so its tools draw on its own screen and keep
/// their own browser profile (sign-ins persist).
pub fn bot_env(info: &Info, bot_id: &str, bot_dir: &Path) -> HashMap<String, String> {
    let profile = bot_dir.join("browser");
    HashMap::from([
        ("DISPLAY".into(), info.display.clone()),
        ("DBUS_SESSION_BUS_ADDRESS".into(), info.bus.clone()),
        ("AGENT_BROWSER_SESSION".into(), browser_session(bot_id)),
        ("AGENT_BROWSER_PROFILE".into(), profile.display().to_string()),
        ("AGENT_BROWSER_HEADED".into(), "1".into()),
        ("AGENT_BROWSER_NO_XVFB".into(), "1".into()),
        ("AGENT_BROWSER_ARGS".into(), "--start-maximized".into()),
        // Lets agent-desktop read GTK, Qt, and Chromium apps on this screen.
        ("QT_ACCESSIBILITY".into(), "1".into()),
        ("ACCESSIBILITY_ENABLED".into(), "1".into()),
    ])
}
