/*
MINE PRODUCTION DASHBOARD - ADVANCED JAVASCRIPT
Mulungushi University | Mwape Kelvin
Interactive dashboard with real-time updates
*/

// ============================================================
// GLOBAL VARIABLES
// ============================================================

let refreshInterval;
let currentStageFilter = 'All';
let currentTableLimit = 30;
let lastBottleneckCount = 0;
let audioContext = null;

// ============================================================
// INITIALIZATION
// ============================================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Mine Production Dashboard Initialized');
    
    setupEventListeners();
    loadAllData();
    
    // Auto-refresh every 30 seconds
    refreshInterval = setInterval(loadAllData, 30000);
});

// ============================================================
// EVENT LISTENERS
// ============================================================

function setupEventListeners() {
    // Refresh button
    const refreshBtn = document.getElementById('btn-refresh');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            loadAllData();
            showToast('🔄 Refreshing dashboard data...', 'info');
        });
    }
    
    // Export button
    const exportBtn = document.getElementById('btn-export');
    if (exportBtn) {
        exportBtn.addEventListener('click', exportToCSV);
    }
    
    // Clear alerts button
    const clearBtn = document.getElementById('btn-clear-alerts');
    if (clearBtn) {
        clearBtn.addEventListener('click', function() {
            clearAlertsDisplay();
            showToast('✅ Alerts cleared', 'success');
        });
    }
    
    // Filter buttons
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            currentStageFilter = this.getAttribute('data-stage');
            loadTableData();
        });
    });
    
    // Show more button
    const showMoreBtn = document.getElementById('btn-show-more');
    if (showMoreBtn) {
        showMoreBtn.addEventListener('click', function() {
            currentTableLimit += 30;
            loadTableData();
            showToast(`Showing ${currentTableLimit} records`, 'info');
        });
    }
    
    // Show less button
    const showLessBtn = document.getElementById('btn-show-less');
    if (showLessBtn) {
        showLessBtn.addEventListener('click', function() {
            currentTableLimit = Math.max(20, currentTableLimit - 30);
            loadTableData();
            showToast(`Showing ${currentTableLimit} records`, 'info');
        });
    }
}

// ============================================================
// MAIN DATA LOADING FUNCTION
// ============================================================

async function loadAllData() {
    try {
        await Promise.all([
            loadStats(),
            loadKPIs(),
            loadThroughput(),
            loadBottlenecks(),
            loadTableData(),
            loadDailySummary(),
            loadBottleneckHistory()
        ]);
        
        updateTimestamp();
        console.log('✅ All data loaded successfully');
        
    } catch (error) {
        console.error('Error loading data:', error);
        showToast('Error loading data. Check server connection.', 'error');
    }
}

// ============================================================
// STATISTICS
// ============================================================

async function loadStats() {
    const response = await fetch('/api/stats');
    const data = await response.json();
    
    if (data.success) {
        document.getElementById('stat-records').innerHTML = data.total_records.toLocaleString();
        document.getElementById('stat-stages').innerHTML = data.total_stages;
        document.getElementById('stat-avg-throughput').innerHTML = data.avg_throughput.toLocaleString();
        document.getElementById('stat-active-bottlenecks').innerHTML = data.active_bottlenecks;
        document.getElementById('stat-total-bottlenecks').innerHTML = data.total_bottlenecks;
        document.getElementById('stat-best-stage').innerHTML = data.best_stage;
        document.getElementById('stat-best-rate').innerHTML = data.best_stage_rate;
        document.getElementById('stat-best-shift').innerHTML = data.best_shift;
        document.getElementById('stat-best-shift-eff').innerHTML = data.best_shift_efficiency;
    }
}

// ============================================================
// KPIs (Objective 2)
// ============================================================

async function loadKPIs() {
    const response = await fetch('/api/kpis');
    const data = await response.json();
    
    if (data.success) {
        document.getElementById('total-production').innerHTML = data.total_actual.toLocaleString();
        document.getElementById('target-percent').innerHTML = data.overall_percent + '%';
        
        // Set color based on performance
        const percentElem = document.getElementById('target-percent');
        if (data.overall_percent >= 90) {
            percentElem.style.color = '#28a745';
        } else if (data.overall_percent >= 75) {
            percentElem.style.color = '#ffc107';
        } else {
            percentElem.style.color = '#dc3545';
        }
        
        updateTargetActualChart(data.stages);
    }
}

function updateTargetActualChart(stages) {
    const container = document.getElementById('target-actual-chart');
    if (!container) return;
    
    if (!stages || stages.length === 0) {
        container.innerHTML = '<div class="loading">No data available</div>';
        return;
    }
    
    let html = '';
    for (let stage of stages) {
        let barClass = '';
        if (stage.efficiency >= 90) barClass = '';
        else if (stage.efficiency >= 75) barClass = 'warning';
        else barClass = 'critical';
        
        html += `
            <div class="bar-item">
                <div class="bar-label">
                    <span><strong>${stage.stage}</strong></span>
                    <span>${stage.actual.toLocaleString()} / ${stage.target.toLocaleString()} tons (${stage.efficiency}%)</span>
                </div>
                <div class="bar-bg">
                    <div class="bar-fill ${barClass}" style="width: ${Math.min(stage.efficiency, 100)}%">
                        ${stage.efficiency}%
                    </div>
                </div>
            </div>
        `;
    }
    container.innerHTML = html;
}

// ============================================================
// THROUGHPUT (Objective 2)
// ============================================================

async function loadThroughput() {
    const response = await fetch('/api/throughput');
    const data = await response.json();
    
    if (data.success && data.throughput) {
        updateThroughputChart(data.throughput);
    }
}

function updateThroughputChart(throughput) {
    const container = document.getElementById('throughput-chart');
    if (!container) return;
    
    let html = '';
    for (let item of throughput) {
        let barClass = '';
        if (item.status_class === 'excellent') barClass = '';
        else if (item.status_class === 'normal') barClass = '';
        else if (item.status_class === 'warning') barClass = 'warning';
        else barClass = 'critical';
        
        let statusIcon = '';
        if (item.status_class === 'excellent') statusIcon = '✅';
        else if (item.status_class === 'normal') statusIcon = '✓';
        else if (item.status_class === 'warning') statusIcon = '⚠️';
        else statusIcon = '🔴';
        
        html += `
            <div class="bar-item">
                <div class="bar-label">
                    <span><strong>${item.stage}</strong> ${statusIcon} <span class="status-${item.status_class}">${item.status}</span></span>
                    <span>${item.rate} / ${item.threshold} t/h (${item.percent}%)</span>
                </div>
                <div class="bar-bg">
                    <div class="bar-fill ${barClass}" style="width: ${Math.min(item.percent, 100)}%">
                        ${item.rate} t/h
                    </div>
                </div>
            </div>
        `;
    }
    container.innerHTML = html;
}

// ============================================================
// BOTTLENECKS (Objective 3)
// ============================================================

async function loadBottlenecks() {
    const response = await fetch('/api/bottlenecks');
    const data = await response.json();
    
    if (data.success) {
        updateAlertsPanel(data.bottlenecks, data.count);
        
        // Play sound for new bottlenecks
        if (data.count > lastBottleneckCount && data.count > 0) {
            playAlertSound();
            showToast(`⚠️ ${data.count} new bottleneck(s) detected!`, 'warning');
        }
        lastBottleneckCount = data.count;
    }
}

function updateAlertsPanel(bottlenecks, count) {
    const container = document.getElementById('alerts-list');
    if (!container) return;
    
    if (count === 0) {
        container.innerHTML = `
            <div style="text-align: center; padding: 40px;">
                <div style="font-size: 48px; margin-bottom: 15px;">✅</div>
                <h4 style="color: #28a745;">No Active Bottlenecks</h4>
                <p style="color: #6c757d;">All production stages operating at optimal levels</p>
            </div>
        `;
        return;
    }
    
    let html = '';
    for (let b of bottlenecks) {
        const severityClass = b.severity === 'Critical' ? 'critical' : '';
        
        html += `
            <div class="alert-item ${severityClass}">
                <div class="stage">
                    ${b.severity_icon} ${b.stage} Stage - ${b.severity} BOTTLENECK ALERT
                </div>
                <div class="details">
                    <strong>Current Rate:</strong> ${b.current_rate} t/h | 
                    <strong>Threshold:</strong> ${b.threshold}+ t/h | 
                    <strong>Deficit:</strong> ${b.deficit} t/h
                </div>
                <div class="details">
                    <strong>Production at ${b.severity_score}% of target</strong>
                </div>
                <div class="action">
                    🔧 ACTION REQUIRED: Immediate investigation of ${b.stage} stage
                </div>
            </div>
        `;
    }
    container.innerHTML = html;
}

// ============================================================
// TABLE DATA (Objective 2)
// ============================================================

async function loadTableData() {
    const url = `/api/production_data?limit=${currentTableLimit}&stage=${currentStageFilter}`;
    const response = await fetch(url);
    const data = await response.json();
    
    if (data.success) {
        updateProductionTable(data.data);
        document.getElementById('table-count').innerHTML = data.data.length;
    }
}

function updateProductionTable(records) {
    const tbody = document.getElementById('table-body');
    if (!tbody) return;
    
    if (!records || records.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" style="text-align: center;">No data available</td></tr>';
        return;
    }
    
    let html = '';
    for (let record of records) {
        const rowClass = record.is_bottleneck ? 
            (record.efficiency < 50 ? 'critical-row' : 'bottleneck-row') : '';
        
        const efficiencyClass = record.efficiency >= 90 ? 'text-success' : 
                                (record.efficiency >= 75 ? 'text-warning' : 'text-danger');
        
        html += `
            <tr class="${rowClass}">
                <td>${record.timestamp}</td>
                <td><strong>${record.stage}</strong></td>
                <td>${record.actual.toLocaleString()}</td>
                <td>${record.target.toLocaleString()}</td>
                <td class="${record.throughput < 800 ? 'text-danger' : 'text-success'}">${record.throughput}</td>
                <td class="${efficiencyClass}"><strong>${record.efficiency}%</strong></td>
                <td>${record.shift}</td>
                <td>${record.supervisor}</td>
                <td>${record.equipment}</td>
                <td>${record.ore_grade}</td>
                <td>${record.weather}</td>
                <td>${record.operator}</td>
            </tr>
        `;
    }
    tbody.innerHTML = html;
}

// ============================================================
// DAILY SUMMARY
// ============================================================

async function loadDailySummary() {
    const response = await fetch('/api/daily_summary');
    const data = await response.json();
    
    if (data.success) {
        updateSummaryTable(data.summary);
    }
}

function updateSummaryTable(summary) {
    const tbody = document.getElementById('summary-body');
    if (!tbody) return;
    
    if (!summary || summary.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No data available</td></tr>';
        return;
    }
    
    let html = '';
    for (let item of summary) {
        let efficiencyClass = '';
        if (item.efficiency >= 90) efficiencyClass = 'text-success';
        else if (item.efficiency >= 75) efficiencyClass = 'text-warning';
        else efficiencyClass = 'text-danger';
        
        html += `
            <tr>
                <td><strong>${item.date}</strong></td>
                <td>${item.actual.toLocaleString()}</td>
                <td>${item.target.toLocaleString()}</td>
                <td class="${efficiencyClass}"><strong>${item.efficiency}%</strong></td>
                <td>${item.bottlenecks}</td>
            </tr>
        `;
    }
    tbody.innerHTML = html;
}

// ============================================================
// BOTTLENECK HISTORY
// ============================================================

async function loadBottleneckHistory() {
    const response = await fetch('/api/bottleneck_history');
    const data = await response.json();
    
    if (data.success) {
        updateHistoryTable(data.history);
    }
}

function updateHistoryTable(history) {
    const tbody = document.getElementById('history-body');
    if (!tbody) return;
    
    if (!history || history.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">No history available</td></tr>';
        return;
    }
    
    let html = '';
    for (let item of history) {
        let severityClass = '';
        if (item.severity === 'Critical') severityClass = 'text-danger';
        else if (item.severity === 'Severe') severityClass = 'text-warning';
        else severityClass = 'text-info';
        
        html += `
            <tr>
                <td><strong>${item.stage}</strong></td>
                <td>${item.time}</td>
                <td class="${severityClass}"><strong>${item.severity}</strong></td>
                <td>${item.score}%</td>
                <td>${item.cause}</td>
                <td>${item.action}</td>
            </tr>
        `;
    }
    tbody.innerHTML = html;
}

// ============================================================
// UTILITY FUNCTIONS
// ============================================================

function exportToCSV() {
    const rows = document.querySelectorAll('#table-body tr');
    if (rows.length === 0 || rows[0].innerHTML.includes('No data')) {
        showToast('No data to export', 'warning');
        return;
    }
    
    let csv = 'Timestamp,Stage,Actual (tons),Target (tons),Throughput (t/h),Efficiency (%),Shift,Supervisor,Equipment,Ore Grade,Weather,Operator\n';
    
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length === 12) {
            const rowData = Array.from(cells).map(cell => {
                let text = cell.innerText.trim();
                if (text.includes(',') || text.includes('"')) {
                    text = `"${text.replace(/"/g, '""')}"`;
                }
                return text;
            }).join(',');
            csv += rowData + '\n';
        }
    });
    
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.href = url;
    link.setAttribute('download', `mine_production_${new Date().toISOString().slice(0,19)}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    
    showToast('✅ Data exported successfully!', 'success');
}

function clearAlertsDisplay() {
    const container = document.getElementById('alerts-list');
    if (container) {
        container.innerHTML = `
            <div style="text-align: center; padding: 40px;">
                <div style="font-size: 48px;">✅</div>
                <h4>Alerts Cleared</h4>
                <p>Dashboard will refresh with new alerts in 30 seconds</p>
            </div>
        `;
    }
}

function playAlertSound() {
    try {
        if (!audioContext) {
            audioContext = new (window.AudioContext || window.webkitAudioContext)();
        }
        
        const oscillator = audioContext.createOscillator();
        const gain = audioContext.createGain();
        
        oscillator.connect(gain);
        gain.connect(audioContext.destination);
        
        oscillator.frequency.value = 880;
        gain.gain.value = 0.15;
        
        oscillator.start();
        gain.gain.exponentialRampToValueAtTime(0.00001, audioContext.currentTime + 0.8);
        
        setTimeout(() => oscillator.stop(), 800);
        
    } catch(e) {
        console.log('Audio not supported');
    }
}

function showToast(message, type = 'info') {
    let toastContainer = document.getElementById('toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        toastContainer.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            z-index: 9999;
            display: flex;
            flex-direction: column;
            gap: 10px;
        `;
        document.body.appendChild(toastContainer);
    }
    
    const toast = document.createElement('div');
    const bgColor = type === 'error' ? '#dc3545' : type === 'warning' ? '#ffc107' : type === 'success' ? '#28a745' : '#2a6e4b';
    const textColor = type === 'warning' ? '#2c3e50' : 'white';
    
    toast.style.cssText = `
        background: ${bgColor};
        color: ${textColor};
        padding: 12px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        animation: slideInRight 0.3s ease;
        font-size: 14px;
        font-weight: 500;
        max-width: 350px;
        word-wrap: break-word;
    `;
    toast.innerHTML = message;
    
    toastContainer.appendChild(toast);
    
    setTimeout(() => {
        toast.style.animation = 'slideOutRight 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

function updateTimestamp() {
    const now = new Date();
    const elem = document.getElementById('refresh-time');
    if (elem) {
        elem.innerHTML = `Last updated: ${now.toLocaleDateString()} ${now.toLocaleTimeString()}`;
    }
}

// Add CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideInRight {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    @keyframes slideOutRight {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
    .text-success { color: #28a745; font-weight: bold; }
    .text-warning { color: #ffc107; font-weight: bold; }
    .text-danger { color: #dc3545; font-weight: bold; }
    .text-info { color: #17a2b8; font-weight: bold; }
`;
document.head.appendChild(style);