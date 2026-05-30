package com.kafka.active.web;

import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Profile("!test")
public class FailoverTestPageController {

	@GetMapping(value = "/failover/test", produces = MediaType.TEXT_HTML_VALUE)
	@ResponseBody
	public String page() {
		return HTML;
	}

	private static final String HTML =
			"""
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Failover 유실·중복 검증</title>
<style>
  :root { --bg:#0b0f14; --panel:#141c28; --text:#e8eef7; --muted:#8b9bb4; --accent:#4db0ff; --ok:#45d483; --warn:#f0b429; --bad:#ff6b7a; --border:#243044; }
  body { font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 1rem 1.2rem 2rem; max-width: 960px; }
  h1 { font-size: 1.25rem; margin: 0 0 .5rem; }
  .sub { color: var(--muted); font-size: .85rem; line-height: 1.5; margin-bottom: 1rem; }
  .steps { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: .75rem 1rem; margin-bottom: 1rem; font-size: .82rem; color: var(--muted); }
  .steps ol { margin: .4rem 0 0 1.1rem; padding: 0; }
  .metrics { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: .5rem; margin-bottom: 1rem; }
  .metric { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: .55rem .65rem; }
  .metric label { display: block; font-size: .72rem; color: var(--muted); }
  .metric b { font-size: 1.35rem; }
  .metric.ok b { color: var(--ok); }
  .metric.bad b { color: var(--bad); }
  .metric.warn b { color: var(--warn); }
  .toolbar { display: flex; flex-wrap: wrap; gap: .45rem; margin-bottom: 1rem; }
  button { background: #243044; border: 1px solid var(--border); color: var(--text); padding: .45rem .75rem; border-radius: 8px; cursor: pointer; font-size: .84rem; }
  button:hover { background: #2d3d56; }
  button.primary { background: #1a4a6e; border-color: var(--accent); }
  button.danger { border-color: var(--bad); color: var(--bad); }
  .run-id { font-family: ui-monospace, monospace; font-size: .78rem; color: var(--accent); word-break: break-all; margin-bottom: .75rem; }
  .lists { display: grid; grid-template-columns: 1fr 1fr; gap: .75rem; }
  @media (max-width: 700px) { .lists { grid-template-columns: 1fr; } }
  .list-box { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: .6rem; }
  .list-box h3 { margin: 0 0 .4rem; font-size: .82rem; }
  .list-box pre { margin: 0; font-size: .72rem; max-height: 120px; overflow: auto; color: var(--muted); white-space: pre-wrap; }
  .err { color: var(--bad); margin-top: .5rem; font-size: .84rem; }
  .status { font-size: .8rem; color: var(--muted); margin-bottom: .5rem; }
</style>
</head>
<body>
<h1>Consumer Failover — 유실·중복 검증</h1>
<p class="sub">
  테스트 중 produce는 <b>Primary 클러스터(A) 단일</b>로만 발행합니다 (<code>testRunId</code> 포함).
  consume 시마다 ClickHouse <code>failover_message_*</code> 테이블에 기록됩니다.
  <code>missing</code> = produce 했으나 한 번도 consume 안 된 id · <code>duplicate</code> = 같은 id 2회 이상 consume.
</p>
<p class="sub" id="modeHint"></p>
<div class="steps">
  <b>권장 시나리오</b>
  <ol>
    <li><b>테스트 시작</b> → Primary에서 produce 20건 → PRIMARY 소비 확인</li>
    <li><b>Failover (Standby)</b> → produce 10건 → STANDBY 소비 (B 토픽만 해당)</li>
    <li><b>Failback (Primary)</b> → produce 10건 → PRIMARY 소비</li>
    <li><b>테스트 종료</b> → 요약 CH 적재 · missing/duplicate 확인</li>
  </ol>
</div>
<div class="status" id="status">run: 없음</div>
<div class="run-id" id="runId"></div>
<div class="metrics" id="metrics"></div>
<div class="toolbar">
  <button class="primary" type="button" id="btn-start">1. 테스트 시작</button>
  <button type="button" id="btn-produce">2. Produce 20</button>
  <button type="button" id="btn-standby">3. Failover Standby</button>
  <button type="button" id="btn-produce2">4. Produce 10</button>
  <button type="button" id="btn-primary">5. Failback Primary</button>
  <button type="button" id="btn-produce3">6. Produce 10</button>
  <button class="danger" type="button" id="btn-finish">7. 테스트 종료</button>
  <button type="button" id="btn-refresh">새로고침</button>
</div>
<div class="lists">
  <div class="list-box"><h3>유실 샘플 (missing)</h3><pre id="missing"></pre></div>
  <div class="list-box"><h3>중복 샘플 (duplicate id)</h3><pre id="dup"></pre></div>
</div>
<div id="err" class="err"></div>
<script>
async function api(method, path) {
  const res = await fetch(path, { method });
  const text = await res.text();
  if (!res.ok) throw new Error(text || res.status);
  return text ? JSON.parse(text) : {};
}
function renderReport(r, active) {
  document.getElementById('status').textContent = active
    ? 'run: ' + r.status + ' · consumer: ' + r.activeConsumerRole + ' · produce→' + r.produceTargetCluster
    : 'run: 종료됨';
  document.getElementById('runId').textContent = r.runId || '';
  const miss = r.missingCount || 0;
  const dup = r.duplicateConsumeCount || 0;
  document.getElementById('metrics').innerHTML =
    '<div class="metric"><label>produced</label><b>' + r.producedCount + '</b></div>' +
    '<div class="metric"><label>consumed (events)</label><b>' + r.consumedEvents + '</b></div>' +
    '<div class="metric"><label>unique ids</label><b>' + r.consumedUnique + '</b></div>' +
    '<div class="metric ' + (miss === 0 ? 'ok' : 'bad') + '"><label>missing</label><b>' + miss + '</b></div>' +
    '<div class="metric ' + (dup === 0 ? 'ok' : 'warn') + '"><label>duplicate reads</label><b>' + dup + '</b></div>';
  document.getElementById('missing').textContent = (r.missingSampleIds || []).join('\\n') || '(없음)';
  document.getElementById('dup').textContent = (r.duplicateSampleIds || []).join('\\n') || '(없음)';
}
async function loadMode() {
  try {
    const m = await api('GET', '/api/failover/mode');
    const el = document.getElementById('modeHint');
    if (m.consumerMode === 'proxy') {
      el.innerHTML = '<b>모드: HAProxy proxy</b> — Failover/Standby 버튼 비활성. Cluster <b>A 중지</b> 후 HAProxy가 B(backup)로 넘기면 consumer 재연결. bootstrap: <code>' + m.proxyBootstrap + '</code>';
      ['btn-standby','btn-primary'].forEach(id => { const b = document.getElementById(id); if (b) { b.disabled = true; b.title = 'proxy 모드'; } });
    } else {
      el.innerHTML = '<b>모드: dual-listener</b> — Spring이 listener Primary/Standby 전환 (데모). 인프라 failover는 <code>consumer-mode=proxy</code> + HAProxy 권장.';
    }
  } catch (_) {}
}
async function refresh() {
  document.getElementById('err').textContent = '';
  try {
    const d = await api('GET', '/api/failover/test/active');
    if (d.active && d.report) renderReport(d.report, true);
    else {
      document.getElementById('status').textContent = 'run: 없음 (테스트 시작 버튼)';
      document.getElementById('runId').textContent = '';
      document.getElementById('metrics').innerHTML = '';
    }
  } catch (e) { document.getElementById('err').textContent = e.message; }
}
async function act(path) {
  document.getElementById('err').textContent = '';
  try {
    const d = await api('POST', path);
    const r = d.report || d;
    if (r && r.runId) renderReport(r, d.active !== false);
    await refresh();
  } catch (e) { document.getElementById('err').textContent = e.message; }
}
document.getElementById('btn-start').onclick = () => act('/api/failover/test/start');
document.getElementById('btn-produce').onclick = () => act('/api/failover/test/produce?count=20&prefix=fo-p1');
document.getElementById('btn-standby').onclick = () => act('/api/failover/test/failover/standby');
document.getElementById('btn-produce2').onclick = () => act('/api/failover/test/produce?count=10&prefix=fo-p2');
document.getElementById('btn-primary').onclick = () => act('/api/failover/test/failback/primary');
document.getElementById('btn-produce3').onclick = () => act('/api/failover/test/produce?count=10&prefix=fo-p3');
document.getElementById('btn-finish').onclick = () => act('/api/failover/test/finish');
document.getElementById('btn-refresh').onclick = refresh;
loadMode();
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
""";
}
