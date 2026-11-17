//! OpenCode CLI: `opencode run --format json --standalone --auto`.
//! Resumes with `--session <id>` (it creates the session when absent).
//! There is no system-prompt flag, so the system prompt is prepended to the prompt
//! every run. Verified against opencode 2.3.0: one JSON object per line, `text` parts
//! stream deltas, `tool_use` parts carry the tool name and state.

use super::{describe, str_of, Adapter, AgentEvent, ModelOption, RunInput};
use serde_json::Value;

#[derive(Default)]
pub struct Opencode {
    saw_session: bool,
}

impl Adapter for Opencode {
    fn bin(&self) -> &'static str {
        "opencode"
    }

    fn args(&mut self, input: &RunInput, _out: &mut Vec<AgentEvent>) -> Vec<String> {
        let mut a: Vec<String> = ["run", "--format", "json", "--standalone", "--auto"]
            .map(String::from)
            .into();
        if let Some(id) = &input.session_id {
            a.extend(["--session".into(), id.clone()]);
        }
        if let Some(m) = &input.model {
            // OpenCode selects a variant with `provider/model#variant`.
            let model = match &input.effort {
                Some(e) => format!("{m}#{e}"),
                None => m.clone(),
            };
            a.extend(["--model".into(), model]);
        }
        // No system-prompt flag: the prompt carries the identity and rules.
        let mut p = format!("{}\n\n{}", input.system.trim(), input.prompt);
        // A leading "@" would make OpenCode read a file, so pad it.
        if p.starts_with('@') {
            p.insert(0, ' ');
        }
        a.push(p);
        a
    }

    fn on_event(&mut self, e: &Value, out: &mut Vec<AgentEvent>) {
        // Every event carries the run's session id; save it once.
        if !self.saw_session {
            let id = str_of(e, "sessionID");
            if !id.is_empty() {
                self.saw_session = true;
                out.push(AgentEvent::Session(id.into()));
            }
        }
        match str_of(e, "type") {
            // Streaming piece of the current text block; the runner aggregates these.
            "text" => {
                let t = str_of(&e["part"], "text");
                if !t.is_empty() {
                    out.push(AgentEvent::TextDelta(t.into()));
                }
            }
            "tool_use" => {
                let part = &e["part"];
                out.push(AgentEvent::Tool {
                    name: str_of(part, "tool").into(),
                    detail: describe(&part["state"]["input"]),
                });
            }
            "error" => out.push(AgentEvent::Error(error_message(e))),
            "step_finish" if str_of(&e["part"], "reason") == "error" => {
                out.push(AgentEvent::Error(error_message(e)))
            }
            _ => {}
        }
    }

    /// From `opencode models`: one `provider/model` selector per line, plain text.
    fn models(&self) -> Vec<ModelOption> {
        super::output("opencode", &["models"])
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.chars().any(char::is_whitespace))
            .map(|l| ModelOption {
                id: l.into(),
                label: l.into(),
                efforts: vec![],
                aliases: vec![],
            })
            .collect()
    }
}

/// Best effort one-line reason: `error` may be a string or an object, and some
/// events only carry `message`.
fn error_message(e: &Value) -> String {
    let msg = match &e["error"] {
        Value::String(s) => Some(s.as_str()),
        Value::Object(o) => ["message", "reason", "name"]
            .iter()
            .find_map(|k| o.get(*k).and_then(Value::as_str)),
        _ => None,
    };
    msg.or(e["message"].as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("opencode failed")
        .into()
}

#[cfg(test)]
mod tests {
    use super::Opencode;
    use crate::adapters::{Adapter, AgentEvent};
    use serde_json::{json, Value};

    fn feed(a: &mut Opencode, e: Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();
        a.on_event(&e, &mut out);
        out
    }

    /// An adapter that already saved the session, so only the event under test is new.
    fn started() -> Opencode {
        let mut a = Opencode::default();
        let start = json!({"type":"step_start","sessionID":"ses_abc","part":{"type":"step-start"}});
        a.on_event(&start, &mut Vec::new());
        a
    }

    /// Asserts exactly one event and returns it.
    fn one(mut events: Vec<AgentEvent>) -> AgentEvent {
        assert_eq!(events.len(), 1, "expected one event");
        events.remove(0)
    }

    #[test]
    fn saves_the_session_once() {
        let mut a = Opencode::default();
        let start = json!({"type":"step_start","sessionID":"ses_abc","part":{"type":"step-start"}});
        match one(feed(&mut a, start)) {
            AgentEvent::Session(id) => assert_eq!(id, "ses_abc"),
            _ => panic!("expected a session event"),
        }
        // Later events carry the same id and must not repeat it.
        let text = json!({"type":"text","sessionID":"ses_abc","part":{"type":"text","text":"OK"}});
        match one(feed(&mut a, text)) {
            AgentEvent::TextDelta(t) => assert_eq!(t, "OK"),
            _ => panic!("expected a text delta"),
        }
    }

    #[test]
    fn maps_text_as_a_delta() {
        let mut a = started();
        let e = json!({"type":"text","sessionID":"ses_abc","part":{"type":"text","text":"hello"}});
        match one(feed(&mut a, e)) {
            AgentEvent::TextDelta(t) => assert_eq!(t, "hello"),
            _ => panic!("expected a text delta"),
        }
    }

    #[test]
    fn maps_tool_use_to_tool_event() {
        let mut a = started();
        let e = json!({"type":"tool_use","sessionID":"ses_abc","part":{"type":"tool","tool":"glob",
            "state":{"status":"completed","input":{"pattern":"*","path":"/repo"},"output":"a"}}});
        match one(feed(&mut a, e)) {
            AgentEvent::Tool { name, detail } => {
                assert_eq!(name, "glob");
                assert_eq!(detail, "/repo");
            }
            _ => panic!("expected a tool event"),
        }
    }

    #[test]
    fn maps_errors_from_string_object_and_message() {
        let mut a = started();
        let s = json!({"type":"error","sessionID":"ses_abc","error":"rate limited"});
        assert!(matches!(one(feed(&mut a, s)), AgentEvent::Error(m) if m == "rate limited"));
        let o = json!({"type":"error","sessionID":"ses_abc","error":{"message":"bad model"}});
        assert!(matches!(one(feed(&mut a, o)), AgentEvent::Error(m) if m == "bad model"));
        let m = json!({"type":"error","sessionID":"ses_abc","message":"boom"});
        assert!(matches!(one(feed(&mut a, m)), AgentEvent::Error(m) if m == "boom"));
    }

    #[test]
    fn maps_failed_step_finish_to_error() {
        let mut a = started();
        let e = json!({"type":"step_finish","sessionID":"ses_abc","part":{"type":"step-finish","reason":"error"}});
        assert!(matches!(one(feed(&mut a, e)), AgentEvent::Error(m) if m == "opencode failed"));
        let ok = json!({"type":"step_finish","sessionID":"ses_abc","part":{"type":"step-finish","reason":"stop"}});
        assert!(feed(&mut a, ok).is_empty());
    }

    #[test]
    fn args_carry_session_model_variant_and_system_prompt() {
        use crate::adapters::RunInput;
        use std::collections::HashMap;
        let mut a = Opencode::default();
        let input = RunInput {
            prompt: "hi".into(),
            system: "You are Bot.\n".into(),
            session_id: Some("ses_abc".into()),
            cwd: "/tmp".into(),
            model: Some("zai/glm-5.3".into()),
            effort: Some("high".into()),
            env: HashMap::new(),
            state_dir: "/tmp/bot".into(),
        };
        let mut out = Vec::new();
        let args = a.args(&input, &mut out);
        assert_eq!(&args[..4], ["run", "--format", "json", "--standalone"]);
        assert!(args.contains(&"--auto".to_string()));
        let at = |k: &str| {
            args.iter()
                .position(|x| x == k)
                .map(|i| args[i + 1].clone())
        };
        assert_eq!(at("--session").as_deref(), Some("ses_abc"));
        assert_eq!(at("--model").as_deref(), Some("zai/glm-5.3#high"));
        assert_eq!(args.last().unwrap(), "You are Bot.\n\nhi");
    }
}
