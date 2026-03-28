// ── Utilities ─────────────────────────────────────────────────────────────────

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ── Auto-refresh stats ───────────────────────────────────────────────────────

async function refreshStats() {
    try {
        const resp = await fetch('/api/stats');
        if (!resp.ok) return;
        const stats = await resp.json();

        const lastRun = document.getElementById('last-run');
        if (lastRun && stats.last_run_at) {
            lastRun.textContent = stats.last_run_at.slice(0, 19).replace('T', ' ');
        }
    } catch (e) {
        // Silent fail — stale data is fine
    }
}

// ── Activity feed ────────────────────────────────────────────────────────────

async function loadActivity() {
    const feed = document.getElementById('activity-feed');
    if (!feed) return;

    try {
        const resp = await fetch('/api/activity');
        if (!resp.ok) { feed.textContent = 'Unavailable'; return; }
        const items = await resp.json();

        if (!items.length) {
            feed.textContent = 'No activity yet';
            return;
        }

        feed.innerHTML = '';
        items.forEach(item => {
            const row = document.createElement('div');
            row.className = 'activity-item';

            const statusClass = item.status === 'OK' ? 'ok' : 'error';
            const dot = document.createElement('span');
            dot.className = `activity-status-${statusClass}`;
            dot.textContent = item.status === 'OK' ? '\u25CF' : '\u2715';

            const step = document.createElement('span');
            step.className = 'activity-step';
            step.textContent = item.step_name || '';

            const time = document.createElement('span');
            time.className = 'activity-time';
            time.textContent = (item.run_at || '').slice(11, 16);

            row.appendChild(dot);
            row.appendChild(step);
            row.appendChild(time);
            feed.appendChild(row);
        });
    } catch (e) {
        feed.textContent = 'Failed to load';
    }
}

// ── Sort narrative cards ─────────────────────────────────────────────────────

function initSortControls() {
    const grid = document.getElementById('narrative-grid');
    if (!grid) return;

    const btns = document.querySelectorAll('.sort-btn');

    btns.forEach(btn => {
        btn.addEventListener('click', () => {
            btns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            const sortBy = btn.dataset.sort;
            const cards = Array.from(grid.querySelectorAll('.narrative-card-link'));

            cards.sort((a, b) => {
                const ca = a.querySelector('.narrative-card');
                const cb = b.querySelector('.narrative-card');

                if (sortBy === 'ns') {
                    return parseFloat(cb.dataset.ns || 0) - parseFloat(ca.dataset.ns || 0);
                } else if (sortBy === 'docs') {
                    return parseInt(cb.dataset.docs || 0) - parseInt(ca.dataset.docs || 0);
                } else if (sortBy === 'age') {
                    const da = ca.dataset.age || '';
                    const db = cb.dataset.age || '';
                    return da < db ? 1 : -1; // newest first
                }
                return 0;
            });

            cards.forEach(card => grid.appendChild(card));
        });
    });
}

// ── Init ─────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    loadActivity();
    initSortControls();
    setInterval(refreshStats, 60000);
});
