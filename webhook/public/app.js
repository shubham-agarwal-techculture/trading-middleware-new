        const API_BASE = window.API_BASE || 'http://localhost:5002';
        let historyCache = [];

        /* ---------- theme ---------- */
        function applyTheme(theme) {
            document.documentElement.setAttribute('data-theme', theme);
            const btn = document.getElementById('theme-btn');
            if (btn) btn.textContent = theme === 'dark' ? '\u2600\uFE0F' : '\u{1F319}';
        }
        function toggleTheme() {
            const current = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('theme', next);
            applyTheme(next);
        }
        (function initTheme() {
            const saved = localStorage.getItem('theme');
            const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
            applyTheme(saved || (prefersDark ? 'dark' : 'light'));
        })();

        /* ---------- helpers ---------- */
        function fmtNum(v, digits = 2) {
            if (v === null || v === undefined || Number.isNaN(Number(v))) return '\u2014';
            return Number(v).toLocaleString('en-IN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
        }
        function fmtMoney(v) {
            if (v === null || v === undefined || Number.isNaN(Number(v))) return '\u20B9\u2014';
            const n = Number(v);
            const sign = n < 0 ? '-' : '';
            return `${sign}\u20B9${Math.abs(n).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        }
        function pnlClass(v) {
            if (v === null || v === undefined || Number.isNaN(Number(v))) return '';
            return Number(v) > 0 ? 'pos' : Number(v) < 0 ? 'neg' : '';
        }
        function fmtDate(s) {
            if (!s) return '\u2014';
            const iso = s + (s.includes('+') || s.endsWith('Z') ? '' : 'Z');
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return s;
            return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        }
        function setConnection(online) {
            const dot = document.getElementById('conn-dot');
            const txt = document.getElementById('conn-text');
            dot.className = 'dot ' + (online ? 'online' : 'offline');
            txt.textContent = online ? 'Connected' : 'Disconnected';
        }
        function stamp() {
            document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' });
        }

        /* Best-effort realized P&L from a closed/open record */
        function computePnl(rec) {
            const entry = Number(rec.entry_price ?? rec.avg_price ?? rec.fill_price ?? rec.price ?? rec.limit_price);
            const exit = Number(rec.current_ltp ?? rec.ltp ?? rec.exit_price);
            const qty = Number(rec.qty ?? 1);
            if (!Number.isFinite(entry) || !Number.isFinite(exit) || !Number.isFinite(qty)) return null;
            const side = String(rec.side || 'BUY').toUpperCase();
            return side === 'SELL' ? (entry - exit) * qty : (exit - entry) * qty;
        }

        function switchTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.getElementById(`${tabName}-tab`).classList.add('active');
            document.querySelector(`.tab-btn[data-tab="${tabName}"]`).classList.add('active');
            if (tabName === 'positions') fetchPositions();
            if (tabName === 'alerts') fetchAlerts();
            if (tabName === 'history') fetchHistory();
        }

        /* ---------- positions ---------- */
        async function fetchPositions() {
            try {
                const res = await fetch(`${API_BASE}/positions`);
                const positions = await res.json();
                setConnection(true);
                renderPositions(positions);
                stamp();
            } catch (e) {
                console.error('positions error', e);
                setConnection(false);
            }
        }

        function renderPositions(positions) {
            const body = document.getElementById('positions-body');
            const empty = document.getElementById('positions-empty');
            const table = document.getElementById('positions-table');
            const keys = Object.keys(positions || {});

            let openCount = 0, filledCount = 0, pendingCount = 0, totalUnrealized = 0, hasUnrealized = false;

            if (keys.length === 0) {
                body.innerHTML = '';
                table.classList.add('hidden');
                empty.classList.remove('hidden');
            } else {
                table.classList.remove('hidden');
                empty.classList.add('hidden');
                body.innerHTML = keys.map(key => {
                    const p = positions[key];
                    openCount++;
                    const status = (p.status || '').toUpperCase();
                    const isFilled = status === 'FILLED' || status === 'COMPLETE';
                    if (isFilled) filledCount++; else pendingCount++;

                    const dv = p.display_values || {};
                    let entry = p.entry_price ?? dv.entry_price;
                    let ltp = dv.current_ltp ?? dv.ltp ?? p.current_ltp;
                    let pnl = dv.kind === 'pnl' ? dv.value : computePnl(p);
                    if (pnl !== null && pnl !== undefined && Number.isFinite(Number(pnl))) { totalUnrealized += Number(pnl); hasUnrealized = true; }

                    const seg = p.exchange_segment || '\u2014';
                    const side = (p.side || 'BUY').toUpperCase();
                    const sqBtn = isFilled ? `<button class="btn danger" onclick="squareOff('${key}')">Square Off</button>` : '';

                    return `
                        <tr>
                            <td>
                                <div class="instrument-name">${p.instrument || key}</div>
                                <div class="instrument-sub">ID: ${p.exchange_instrument_id ?? key}</div>
                            </td>
                            <td><span class="badge seg">${seg}</span></td>
                            <td><span class="badge ${side.toLowerCase()}">${side}</span></td>
                            <td class="num">${p.qty ?? '\u2014'}</td>
                            <td class="num">${fmtNum(entry)}</td>
                            <td class="num">${fmtNum(ltp)}</td>
                            <td class="num ${pnlClass(pnl)}">${pnl === null || pnl === undefined ? '\u2014' : fmtMoney(pnl)}</td>
                            <td><span class="badge ${status.toLowerCase()}">${status || 'N/A'}</span></td>
                            <td>${fmtDate(p.opened_at)}</td>
                            <td>${sqBtn}</td>
                        </tr>`;
                }).join('');
            }

            document.getElementById('kpi-open').textContent = openCount;
            document.getElementById('kpi-open-sub').textContent = `${filledCount} filled \u00B7 ${pendingCount} pending`;
            const uEl = document.getElementById('kpi-unrealized');
            uEl.textContent = hasUnrealized ? fmtMoney(totalUnrealized) : '\u20B9\u2014';
            uEl.className = 'value ' + pnlClass(hasUnrealized ? totalUnrealized : null);
        }

        async function squareOff(instrumentKey) {
            if (!confirm('Square off this position?')) return;
            try {
                const res = await fetch(`${API_BASE}/squareoff`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ instrument_key: instrumentKey })
                });
                const result = await res.json();
                if (result.status === 'success') {
                    fetchPositions(); fetchAlerts();
                } else {
                    alert('Error: ' + (result.message || 'square-off failed'));
                }
            } catch (e) {
                console.error(e);
                alert('Error squaring off position');
            }
        }

        /* ---------- new order ---------- */
        let orderMode = 'symbol';
        function setOrderMode(mode) {
            orderMode = mode;
            document.getElementById('mode-symbol').classList.toggle('active', mode === 'symbol');
            document.getElementById('mode-instrument').classList.toggle('active', mode === 'instrument');
            document.getElementById('field-symbol').classList.toggle('hidden', mode !== 'symbol');
            document.getElementById('field-segment').classList.toggle('hidden', mode !== 'instrument');
            document.getElementById('field-instrument').classList.toggle('hidden', mode !== 'instrument');
        }
        function toggleLimitPrice() {
            const isLimit = document.getElementById('o-ordertype').value === 'LIMIT';
            document.getElementById('field-limit').classList.toggle('hidden', !isLimit);
        }
        function updateActionHint() {
            const action = document.getElementById('o-action').value;
            const hint = document.getElementById('action-hint');
            if (!hint) return;
            if (action === 'SELL') {
                hint.textContent = 'SELL closes an existing long (square-off). It will not open a short.';
                hint.classList.add('warn');
            } else {
                hint.textContent = 'BUY opens a long. Shorts are not supported.';
                hint.classList.remove('warn');
            }
        }
        function resetOrderForm() {
            document.getElementById('order-form').reset();
            const r = document.getElementById('order-result');
            r.className = 'order-result';
            r.textContent = '';
            toggleLimitPrice();
            updateActionHint();
        }
        async function submitOrder(evt) {
            evt.preventDefault();
            const resultEl = document.getElementById('order-result');
            const btn = document.getElementById('order-submit');
            const payload = {
                action: document.getElementById('o-action').value,
                position: document.getElementById('o-position').value,
                orderType: document.getElementById('o-ordertype').value,
                productType: document.getElementById('o-product').value,
            };
            const qty = document.getElementById('o-quantity').value;
            if (qty) payload.quantity = Number(qty);
            if (document.getElementById('o-ordertype').value === 'LIMIT') {
                const lp = document.getElementById('o-limit').value;
                if (lp) payload.limitPrice = Number(lp);
            }
            if (orderMode === 'symbol') {
                const sym = document.getElementById('o-symbol').value.trim();
                if (!sym) { showOrderResult(false, 'Symbol is required'); return; }
                payload.symbol = sym;
            } else {
                const seg = document.getElementById('o-segment').value;
                const iid = document.getElementById('o-instrument').value;
                if (!iid) { showOrderResult(false, 'Instrument ID is required'); return; }
                payload.exchange_segment = seg;
                payload.exchange_instrument_id = Number(iid);
            }

            btn.disabled = true; btn.textContent = 'Submitting\u2026';
            try {
                const res = await fetch(`${API_BASE}/signal`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                const status = (data.status || '').toLowerCase();
                const ok = ['pending', 'submitted', 'processing', 'success'].includes(status);
                showOrderResult(ok, `${data.status || 'unknown'}: ${data.message || data.instrument || 'order received'}`);
                fetchPositions(); fetchAlerts();
            } catch (e) {
                showOrderResult(false, 'Request failed: ' + e.message);
            } finally {
                btn.disabled = false; btn.textContent = 'Submit Order';
            }
        }
        function showOrderResult(ok, msg) {
            const r = document.getElementById('order-result');
            r.className = 'order-result ' + (ok ? 'ok' : 'err');
            r.textContent = msg;
        }

        /* ---------- alerts ---------- */
        async function fetchAlerts() {
            try {
                const res = await fetch(`${API_BASE}/alerts`);
                const alerts = await res.json();
                setConnection(true);
                renderAlerts(alerts);
                stamp();
            } catch (e) {
                console.error('alerts error', e);
                setConnection(false);
            }
        }
        function renderAlerts(alerts) {
            const container = document.getElementById('alerts-container');
            if (!alerts || alerts.length === 0) {
                container.innerHTML = '<div class="no-data">No alerts</div>';
                return;
            }
            container.innerHTML = alerts.map(a => `
                <div class="alert-item ${(a.type || '').toLowerCase()}">
                    <div class="alert-timestamp">${fmtDate(a.timestamp)}</div>
                    <div class="alert-message">${a.message || ''}</div>
                </div>`).join('');
        }

        /* ---------- history ---------- */
        async function fetchHistory() {
            try {
                const res = await fetch(`${API_BASE}/history`);
                historyCache = await res.json();
                setConnection(true);
                renderHistory();
                stamp();
            } catch (e) {
                console.error('history error', e);
                setConnection(false);
            }
        }
        function renderHistory() {
            const body = document.getElementById('history-body');
            const empty = document.getElementById('history-empty');
            const table = document.getElementById('history-table');
            const q = (document.getElementById('history-search').value || '').toLowerCase();
            const items = (historyCache || []).filter(i => !q || (i.instrument || '').toLowerCase().includes(q));

            // KPI: realized pnl + win rate over full history (not just filtered)
            let realized = 0, realizedCount = 0, wins = 0;
            (historyCache || []).forEach(i => {
                const pnl = computePnl(i);
                if (pnl !== null) { realized += pnl; realizedCount++; if (pnl > 0) wins++; }
            });
            const rEl = document.getElementById('kpi-realized');
            rEl.textContent = realizedCount ? fmtMoney(realized) : '\u20B9\u2014';
            rEl.className = 'value ' + pnlClass(realizedCount ? realized : null);
            document.getElementById('kpi-trades').textContent = (historyCache || []).length;
            document.getElementById('kpi-winrate').textContent = realizedCount
                ? `Win rate: ${((wins / realizedCount) * 100).toFixed(0)}% (${wins}/${realizedCount})`
                : 'Win rate: \u2014';

            if (items.length === 0) {
                body.innerHTML = '';
                table.classList.add('hidden');
                empty.classList.remove('hidden');
                empty.textContent = q ? 'No matching history' : 'No history available';
                return;
            }
            table.classList.remove('hidden');
            empty.classList.add('hidden');
            body.innerHTML = items.map(i => {
                const status = (i.final_status || i.status || '').toUpperCase();
                const side = (i.side || 'BUY').toUpperCase();
                const pnl = computePnl(i);
                const exit = i.current_ltp ?? i.ltp ?? i.exit_price;
                return `
                    <tr>
                        <td><div class="instrument-name">${i.instrument || '\u2014'}</div><div class="instrument-sub">ID: ${i.exchange_instrument_id ?? '\u2014'}</div></td>
                        <td><span class="badge seg">${i.exchange_segment || '\u2014'}</span></td>
                        <td><span class="badge ${side.toLowerCase()}">${side}</span></td>
                        <td class="num">${i.qty ?? '\u2014'}</td>
                        <td class="num">${fmtNum(i.entry_price)}</td>
                        <td class="num">${fmtNum(exit)}</td>
                        <td class="num ${pnlClass(pnl)}">${pnl === null ? '\u2014' : fmtMoney(pnl)}</td>
                        <td><span class="badge ${status.toLowerCase()}">${status || 'N/A'}</span></td>
                        <td>${fmtDate(i.opened_at)}</td>
                        <td>${fmtDate(i.closed_at)}</td>
                    </tr>`;
            }).join('');
        }

        /* ---------- refresh loop ---------- */
        function refreshAll() {
            fetchPositions();
            fetchAlerts();
            fetchHistory();
        }

        toggleLimitPrice();
        updateActionHint();
        refreshAll();
        setInterval(() => {
            if (!document.getElementById('autorefresh').checked) return;
            fetchPositions();
            fetchAlerts();
            const histActive = document.getElementById('history-tab').classList.contains('active');
            if (histActive) fetchHistory();
        }, 5000);
