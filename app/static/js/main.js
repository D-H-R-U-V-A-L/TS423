document.addEventListener('DOMContentLoaded', () => {


const runEngineBtn = document.getElementById('btn-run-engine');

// -------------------------------
// 🚀 RUN DETECTION ENGINE
// -------------------------------
if (runEngineBtn) {
    runEngineBtn.addEventListener('click', async () => {
        runEngineBtn.innerHTML = '<i class="ph ph-spinner ph-spin"></i> Processing...';
        runEngineBtn.disabled = true;

        try {
            const res = await fetch('/api/run_detection', { method: 'POST' });
            const data = await res.json();

            if (data.status === 'success') {
                alert(`Detection Complete!\nAlerts: ${data.alerts_generated}\nTime: ${data.time_taken_seconds}s`);
                if (document.getElementById('alerts-table')) fetchAlerts();
            }

        } catch (err) {
            console.error(err);
            alert("Error running engine.");
        } finally {
            runEngineBtn.innerHTML = '<i class="ph ph-play"></i> Run Detection Engine';
            runEngineBtn.disabled = false;
        }
    });
}

// Load alerts (DFO Dashboard)
if (document.getElementById('alerts-table')) {
    fetchAlerts();
}

// Load assigned case (Verifier page)
if (document.getElementById('assigned-cases')) {
    loadAssignedCase();
}

// Charts — only run on pages that have BOTH chart canvases (DFO / admin pages).
// The audit page has #chart-leakage-type too, but NOT #chart-district,
// so this guard prevents main.js from pre-claiming the audit canvas.
if (document.getElementById('chart-leakage-type') && document.getElementById('chart-district')) {
    renderCharts();
}


});

// -------------------------------
// 📊 FETCH ALERTS
// -------------------------------
async function fetchAlerts() {
try {
const tbody = document.getElementById('alerts-tbody');


    tbody.innerHTML = `
        <tr><td colspan="7" class="text-center p-3">⏳ Loading alerts...</td></tr>
    `;

    const res = await fetch('/api/alerts');
    const alerts = await res.json();

    document.getElementById('alert-badge').innerText = alerts.length;

    tbody.innerHTML = '';

    let critical = 0, high = 0, pending = 0;

    alerts.forEach(a => {

        if (a.risk_score >= 90) critical++;
        else if (a.risk_score >= 75) high++;

        if (a.status === 'PENDING') pending++;

        const badgeClass = a.risk_score >= 90 ? 'badge-danger' : 'badge-warning';

        let evidenceParsed = {};
        try { evidenceParsed = JSON.parse(a.evidence); } catch (e) {}

        const tr = document.createElement('tr');

        // Action cell — conditional on status
        const CLOSED = ['VERIFIED', 'FALSE_POSITIVE'];
        let actionEl;

        if (CLOSED.includes(a.status)) {
            // Read-only closed pill — no button
            actionEl = document.createElement('span');
            actionEl.style.cssText = `
                display:inline-flex; align-items:center; gap:0.3rem;
                font-size:0.72rem; font-weight:600; padding:0.25rem 0.65rem;
                border-radius:6px; cursor:default;
                background:${a.status === 'VERIFIED' ? 'rgba(16,185,129,0.12)' : 'rgba(107,114,128,0.12)'};
                color:${a.status === 'VERIFIED' ? '#6ee7b7' : '#9ca3af'};
                border:1px solid ${a.status === 'VERIFIED' ? 'rgba(16,185,129,0.3)' : 'rgba(107,114,128,0.3)'};
            `;
            actionEl.innerHTML = `<i class="ph ph-check-circle"></i> ${a.status === 'VERIFIED' ? 'Verified' : 'False Positive'}`;
        } else if (a.status === 'ASSIGNED') {
            actionEl = document.createElement('span');
            actionEl.style.cssText = `
                display:inline-flex; align-items:center; gap:0.3rem;
                font-size:0.72rem; font-weight:600; padding:0.25rem 0.65rem;
                border-radius:6px; cursor:default;
                background:rgba(59,130,246,0.1); color:#93c5fd;
                border:1px solid rgba(59,130,246,0.25);
            `;
            actionEl.innerHTML = `<i class="ph ph-clock"></i> Assigned`;
        } else {
            // PENDING — show Investigate button
            actionEl = document.createElement('button');
            actionEl.className = 'btn btn-primary';
            actionEl.style.padding = '0.4rem 0.8rem';
            actionEl.style.fontSize = '0.75rem';
            actionEl.innerText = 'Investigate';
            actionEl.addEventListener('click', () => {
                openAssignModal(
                    a.id,
                    a.beneficiary_name,
                    (evidenceParsed.detail || a.evidence),
                    a.leakage_type,
                    a.district
                );
            });
        }

        tr.innerHTML = `
            <td><span class="${badgeClass}">${a.risk_score}</span></td>
            <td class="font-bold">#BEN-${a.beneficiary_id}</td>
            <td>${a.beneficiary_name}</td>
            <td>${a.district}</td>
            <td>${a.leakage_type.replace(/_/g, ' ')}</td>
            <td><span style="font-size:0.75rem; color:var(--text-muted);">${a.status}</span></td>
            <td class="action-cell"></td>
        `;

        tr.querySelector('.action-cell').appendChild(actionEl);
        tbody.appendChild(tr);
    });

    document.getElementById('stat-critical').innerText = critical;
    document.getElementById('stat-high').innerText = high;
    document.getElementById('stat-pending').innerText = pending;

    if (alerts.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="text-center p-3 text-muted">
                    No alerts found. Run detection engine.
                </td>
            </tr>
        `;
    }

} catch (e) {
    console.error(e);
}


}

// -------------------------------
// 🧧 ASSIGN MODAL
// -------------------------------
let activeAlertId   = null;
let activeAlertData = {};   // stores full alert context for rich verifier display

window.openAssignModal = function(id, name, evidence, leakageType, district) {
    activeAlertId   = id;
    activeAlertData = { id, name, evidence, leakageType: leakageType || '', district: district || '' };

    document.getElementById('modal-ben-name').innerText = name;
    document.getElementById('modal-evidence').innerText = evidence;

    document.getElementById('assign-modal').classList.add('active');
};

// 🔥 SAVE ASSIGNED ALERT — appends to array so multiple cases accumulate
window.assignCase = function() {
    if (!activeAlertId) {
        alert('Error: No alert selected!');
        return;
    }

    // Load existing queue
    let queue = [];
    try { queue = JSON.parse(localStorage.getItem('assignedAlerts') || '[]'); } catch(e) {}

    // Dedup — don't add the same alert twice
    if (queue.find(c => c.id === activeAlertId)) {
        alert('This case is already in the verifier queue.');
        document.getElementById('assign-modal').classList.remove('active');
        return;
    }

    queue.push(activeAlertData);
    localStorage.setItem('assignedAlerts', JSON.stringify(queue));

    alert(`Case assigned! ${queue.length} case${queue.length !== 1 ? 's' : ''} now in verifier queue.`);
    document.getElementById('assign-modal').classList.remove('active');
    activeAlertId   = null;
    activeAlertData = {};
};

// -------------------------------
// 📍 VERIFIER PAGE
// -------------------------------
function loadAssignedCase() {
    const container = document.getElementById('assigned-cases');
    if (!container) return;

    let queue = [];
    try { queue = JSON.parse(localStorage.getItem('assignedAlerts') || '[]'); } catch(e) {}

    if (!queue.length) {
        container.innerHTML = `<p class="text-muted p-2">No assigned cases. Ask the DFO to assign cases.</p>`;
        return;
    }

    container.innerHTML = queue.map(c => `
        <div class="case-card" id="case-card-${c.id}">
            <div class="case-header">
                <span class="badge-warning">${(c.leakageType || 'ALERT').replace(/_/g,' ')}</span>
                <span class="case-id">Alert #${c.id}</span>
            </div>
            <div class="case-body mt-1">
                <p><strong>Beneficiary:</strong> ${c.name}</p>
                ${c.district ? `<p><strong>District:</strong> ${c.district}</p>` : ''}
                <button class="btn btn-primary mt-1 w-full"
                    onclick="openVerification(${c.id})">
                    Start Field Visit
                </button>
            </div>
        </div>
    `).join('');
}

// -------------------------------
// 📍 GPS + VERIFY
// -------------------------------
window.openVerification = function(id) {
activeAlertId = id;


const modal = document.getElementById('verify-modal');
modal.classList.add('active');

if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(
        (pos) => {
            document.getElementById('gps-status').innerHTML =
                '<i class="ph ph-check-circle text-success"></i> GPS Acquired';

            document.getElementById('lat-long').innerText =
                `${pos.coords.latitude.toFixed(5)}, ${pos.coords.longitude.toFixed(5)}`;

            window.currentPoint = pos.coords;
        },
        () => fallbackGPS()
    );
} else {
    fallbackGPS();
}


};

function fallbackGPS() {
document.getElementById('gps-status').innerHTML =
'<i class="ph ph-warning text-warning"></i> Using Mock Location';


document.getElementById('lat-long').innerText =
    `23.0225, 72.5714 (Ahmedabad)`;

window.currentPoint = { latitude: 23.0225, longitude: 72.5714 };


}

window.submitVerification = async function() {

    if (!activeAlertId) {
        alert("No alert selected!");
        return;
    }

    const status = document.getElementById('verify-status').value;
    const comments = document.getElementById('verify-comments').value;

    try {
        const res = await fetch(`/api/alerts/${activeAlertId}/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                verifier_id: 'Current_User',
                latitude: window.currentPoint?.latitude || 0,
                longitude: window.currentPoint?.longitude || 0,
                comments: comments,
                status: status
            })
        });

        const data = await res.json();

        alert("Verification submitted successfully!");

        document.getElementById('verify-modal').classList.remove('active');

        // Remove only this alert from the queue; keep the rest
        try {
            let queue = JSON.parse(localStorage.getItem('assignedAlerts') || '[]');
            queue = queue.filter(c => c.id !== activeAlertId);
            localStorage.setItem('assignedAlerts', JSON.stringify(queue));
        } catch(e) {}

        activeAlertId = null;

        // Refresh verifier UI
        loadAssignedCase();

    } catch (e) {
        alert("Verification failed.");
    }
};
// -------------------------------
// 📊 CHARTS — Dynamic (live /api/chart-data)
// -------------------------------

// Chart instances kept so we can destroy + rebuild on refresh
const _charts = {};

async function renderCharts() {
    const ctxLeak = document.getElementById('chart-leakage-type');
    const ctxDist = document.getElementById('chart-district');
    if (!ctxLeak || !ctxDist) return;

    // Pulse the live badge (if present)
    const badge = document.getElementById('charts-live-badge');
    if (badge) badge.style.opacity = '0.4';

    let data;
    try {
        const res = await fetch('/api/chart-data');
        if (!res.ok) throw new Error('fetch failed');
        data = await res.json();
    } catch (e) {
        console.error('[charts] Failed to load chart data', e);
        if (badge) badge.style.opacity = '1';
        return;
    }

    // ── Build sorted arrays ──────────────────────────────────────────────────
    const typeLabels   = Object.keys(data.leakage_type_counts);
    const typeValues   = Object.values(data.leakage_type_counts);
    const distLabels   = Object.keys(data.district_counts).sort(
        (a,b) => data.district_counts[b] - data.district_counts[a]).slice(0, 10);
    const distValues   = distLabels.map(d => data.district_counts[d]);
    const trendLabels  = Object.keys(data.daily_trend);
    const trendValues  = Object.values(data.daily_trend);

    const PALETTE = ['#ef4444', '#f59e0b', '#3b82f6', '#8b5cf6', '#10b981', '#f97316', '#ec4899'];

    // ── Leakage Type Doughnut ────────────────────────────────────────────────
    if (_charts.leak) _charts.leak.destroy();
    _charts.leak = new Chart(ctxLeak.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: typeLabels,
            datasets: [{
                data: typeValues,
                backgroundColor: PALETTE.slice(0, typeLabels.length),
                borderWidth: 0,
                hoverOffset: 8
            }]
        },
        options: {
            animation: { animateRotate: true, duration: 700 },
            plugins: {
                legend: { labels: { color: '#f8fafc', font: { size: 12 } } }
            },
            cutout: '65%'
        }
    });

    // ── District Bar ─────────────────────────────────────────────────────────
    if (_charts.dist) _charts.dist.destroy();
    _charts.dist = new Chart(ctxDist.getContext('2d'), {
        type: 'bar',
        data: {
            labels: distLabels,
            datasets: [{
                label: 'Flagged Cases',
                data: distValues,
                backgroundColor: distValues.map(v =>
                    v >= 100 ? 'rgba(239,68,68,0.75)'
                  : v >= 50  ? 'rgba(245,158,11,0.75)'
                  :            'rgba(59,130,246,0.75)'),
                borderRadius: 5
            }]
        },
        options: {
            animation: { duration: 700 },
            scales: {
                y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                x: { ticks: { color: '#94a3b8' }, grid: { display: false } }
            },
            plugins: { legend: { display: false } }
        }
    });

    // ── 7-day Trend Line ─────────────────────────────────────────────────────
    const ctxTrend = document.getElementById('chart-trend');
    if (ctxTrend) {
        if (_charts.trend) _charts.trend.destroy();
        _charts.trend = new Chart(ctxTrend.getContext('2d'), {
            type: 'line',
            data: {
                labels: trendLabels,
                datasets: [{
                    label: 'Alerts Generated',
                    data: trendValues,
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99,102,241,0.15)',
                    borderWidth: 2,
                    pointRadius: 4,
                    pointBackgroundColor: '#6366f1',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                animation: { duration: 700 },
                scales: {
                    y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                    x: { ticks: { color: '#94a3b8' }, grid: { display: false } }
                },
                plugins: { legend: { labels: { color: '#f8fafc' } } }
            }
        });
    }

    if (badge) badge.style.opacity = '1';
}

// Auto-refresh every 30 seconds — only on pages with both chart canvases
if (document.getElementById('chart-leakage-type') && document.getElementById('chart-district')) {
    setInterval(renderCharts, 30_000);
}


async function uploadCSV() {
const fileInput = document.getElementById("csv-file");


if (!fileInput.files.length) {
    alert("Please select a CSV file");
    return;
}

const formData = new FormData();
formData.append("file", fileInput.files[0]);

try {
    const res = await fetch("/api/upload_csv", {
        method: "POST",
        body: formData
    });

    const data = await res.json();

    if (data.error) {
        alert(data.error);
        return;
    }

    alert(`CSV processed!\nAlerts: ${data.alerts_generated}`);

    // Show alerts in table
    const tbody = document.getElementById("alerts-tbody");
    tbody.innerHTML = "";

    data.alerts.forEach(a => {
        const tr = document.createElement("tr");

        tr.innerHTML = `
            <td>--</td>
            <td>--</td>
            <td>${a.name}</td>
            <td>--</td>
            <td>${a.type}</td>
            <td>NEW</td>
            <td>${a.reason}</td>
        `;

        tbody.appendChild(tr);
    });

} catch (err) {
    console.error(err);
    alert("Upload failed");
}


}
