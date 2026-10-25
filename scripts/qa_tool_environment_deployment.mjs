// Wulala-only read-only deployment check. Credentials stay in memory and are
// never placed in argv/URLs/output. A fresh probe device avoids evicting phones.
import fs from 'node:fs'
import crypto from 'node:crypto'

const file = JSON.parse(fs.readFileSync('/Users/wulala/.everything-go-runtime/pairing.json', 'utf8'))
const token = file.devices?.[0]?.token ?? file.paired_token
if (!token) throw new Error('No configured pairing credential; no changes made')
const sessionArg = process.argv.indexOf('--session')
const sessionId = sessionArg >= 0 ? process.argv[sessionArg + 1] : undefined
const requireTools = process.argv.includes('--require-tools')
const listActive = process.argv.includes('--active')
const report = { target: 'wulala:8766', read_only: true }
const ws = new WebSocket('ws://127.0.0.1:8766/')
const timer = setTimeout(() => finish('probe_timeout'), 40_000)
let finished = false
function finish(error) {
  if (finished) return
  finished = true
  clearTimeout(timer)
  if (error) report.error_code = error
  console.log(JSON.stringify(report))
  ws.close()
  process.exitCode = error ? 1 : 0
}
ws.onopen = () => ws.send(JSON.stringify({ type: 'hello', protocol_version: 3, replay_ack: true, connection_probe: true, auth_token: token, device_id: `tool-environment-release-${crypto.randomUUID()}`, device_name: 'Tool environment deployment probe', client_surface: 'qa' }))
ws.onerror = () => finish('connection_failed')
ws.onmessage = ({ data }) => {
  const message = JSON.parse(data)
  if (message.type === 'hello_ack') {
    report.authority = message.instance_id ?? message.authority_instance_id
    report.tool_environment = message.capabilities?.includes('tool_environment_v1') === true
    if (requireTools && !report.tool_environment) return finish('capability_missing')
    ws.send(JSON.stringify({ type: 'request_status' }))
  } else if (message.type === 'status_result') {
    report.sessions_streaming = message.status?.sessions_streaming
    report.queued_commands = message.status?.queued_commands
    report.running_codex_version = message.status?.backend_runtimes?.codex?.running_version
    if (listActive) {
      ws.send(JSON.stringify({ type: 'request_sessions_list' }))
    } else if (sessionId && report.tool_environment) {
      ws.send(JSON.stringify({ type: 'request_tool_environment', session_id: sessionId, request_id: 'deployment-read', tool_environment: { refresh: true } }))
    } else finish()
  } else if (message.type === 'sessions_list' && listActive) {
    report.active_sessions = (message.sessions ?? []).filter(s => s.is_streaming).map(s => ({ id: s.id, backend: s.backend, cwd: s.cwd }))
    finish()
  } else if (message.type === 'tool_environment_snapshot') {
    if (message.error_code) return finish(message.error_code)
    const snapshot = message.snapshot
    report.diagnosis = { state: snapshot?.state, evidence: snapshot?.evidence, coverage: snapshot?.coverage, reload_allowed: snapshot?.reload_allowed, fork_allowed: snapshot?.fork_allowed, services: snapshot?.services?.filter(s => ['node_repl', 'cua_repl'].includes(s.name)).map(s => ({ name: s.name, state: s.state, tool_count: s.tools?.length })) }
    finish()
  } else if (message.type === 'error') finish('bridge_rejected_probe')
}
