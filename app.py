"""
MINE PRODUCTION DASHBOARD - ADVANCED FLASK APPLICATION
Mulungushi University | Mwape Kelvin
Objective 2: Visual Dashboard with Charts
Objective 3: Automatic Bottleneck Detection and Alerts
"""

from flask import Flask, jsonify, render_template, request
import sqlite3
import os
from datetime import datetime, timedelta

app = Flask(__name__)

DB_PATH = 'database/production.db'

def get_db():
    """Get database connection with row factory"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ============================================================
# ROUTES
# ============================================================

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

# ============================================================
# API 1: KPIs (Objective 2)
# ============================================================

@app.route('/api/kpis')
def get_kpis():
    """Get Key Performance Indicators"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Today's production
        cursor.execute('''
            SELECT stage, 
                   SUM(actual_tonnage) as actual, 
                   SUM(target_tonnage) as target,
                   AVG(efficiency_percent) as efficiency
            FROM production_data
            WHERE date = ?
            GROUP BY stage
            ORDER BY stage_order
        ''', (today,))
        
        stages = []
        total_actual = 0
        total_target = 0
        
        for row in cursor.fetchall():
            total_actual += row['actual']
            total_target += row['target']
            stages.append({
                'stage': row['stage'],
                'actual': round(row['actual'], 1),
                'target': round(row['target'], 1),
                'efficiency': round(row['efficiency'], 1) if row['efficiency'] else 0
            })
        
        overall_percent = (total_actual / total_target * 100) if total_target > 0 else 0
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_actual': round(total_actual, 1),
            'total_target': round(total_target, 1),
            'overall_percent': round(overall_percent, 1),
            'stages': stages
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================
# API 2: Throughput (Objective 2)
# ============================================================

@app.route('/api/throughput')
def get_throughput():
    """Get current throughput rates"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Last hour average
        cursor.execute('''
            SELECT stage, 
                   AVG(throughput_rate) as rate,
                   AVG(efficiency_percent) as efficiency
            FROM production_data
            WHERE timestamp >= datetime('now', '-1 hour')
            GROUP BY stage
            ORDER BY stage_order
        ''')
        
        throughput = []
        thresholds = {'Mining': 960, 'Crushing': 920, 'Milling': 880, 'Flotation': 840, 'Smelting': 800}
        
        for row in cursor.fetchall():
            rate = round(row['rate'], 1) if row['rate'] else 0
            threshold = thresholds.get(row['stage'], 800)
            
            if rate >= threshold * 1.1:
                status = 'Excellent'
                status_class = 'excellent'
            elif rate >= threshold:
                status = 'Normal'
                status_class = 'normal'
            elif rate >= threshold * 0.7:
                status = 'Warning'
                status_class = 'warning'
            else:
                status = 'Critical'
                status_class = 'critical'
            
            throughput.append({
                'stage': row['stage'],
                'rate': rate,
                'threshold': threshold,
                'status': status,
                'status_class': status_class,
                'efficiency': round(row['efficiency'], 1) if row['efficiency'] else 0,
                'percent': round((rate / threshold * 100), 1) if threshold else 0
            })
        
        conn.close()
        return jsonify({'success': True, 'throughput': throughput})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================
# API 3: Bottlenecks (Objective 3)
# ============================================================

@app.route('/api/bottlenecks')
def get_bottlenecks():
    """Detect and return active bottlenecks"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Get last 15 minutes average
        cursor.execute('''
            SELECT stage, 
                   AVG(throughput_rate) as rate,
                   AVG(efficiency_percent) as efficiency
            FROM production_data
            WHERE timestamp >= datetime('now', '-15 minutes')
            GROUP BY stage
            ORDER BY stage_order
        ''')
        
        thresholds = {'Mining': 960, 'Crushing': 920, 'Milling': 880, 'Flotation': 840, 'Smelting': 800}
        
        bottlenecks = []
        for row in cursor.fetchall():
            stage = row['stage']
            rate = row['rate'] if row['rate'] else 0
            threshold = thresholds.get(stage, 800)
            
            if rate < threshold:
                severity_score = (rate / threshold) * 100
                if severity_score >= 70:
                    severity = 'Warning'
                    severity_icon = '🟡'
                elif severity_score >= 50:
                    severity = 'Severe'
                    severity_icon = '🟠'
                else:
                    severity = 'Critical'
                    severity_icon = '🔴'
                
                bottlenecks.append({
                    'stage': stage,
                    'current_rate': round(rate, 1),
                    'threshold': threshold,
                    'deficit': round(threshold - rate, 1),
                    'severity': severity,
                    'severity_icon': severity_icon,
                    'severity_score': round(severity_score, 1),
                    'efficiency': round(row['efficiency'], 1) if row['efficiency'] else 0
                })
        
        conn.close()
        return jsonify({'success': True, 'bottlenecks': bottlenecks, 'count': len(bottlenecks)})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================
# API 4: Production Data Table (Objective 2)
# ============================================================

@app.route('/api/production_data')
def get_production_data():
    """Get production data for table"""
    try:
        limit = request.args.get('limit', 50, type=int)
        stage = request.args.get('stage', None)
        
        conn = get_db()
        cursor = conn.cursor()
        
        if stage and stage != 'All':
            cursor.execute('''
                SELECT timestamp, stage, actual_tonnage, target_tonnage, 
                       throughput_rate, efficiency_percent, shift, shift_supervisor,
                       equipment_status, ore_grade, weather_condition, operator_name
                FROM production_data
                WHERE stage = ?
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (stage, limit))
        else:
            cursor.execute('''
                SELECT timestamp, stage, actual_tonnage, target_tonnage, 
                       throughput_rate, efficiency_percent, shift, shift_supervisor,
                       equipment_status, ore_grade, weather_condition, operator_name
                FROM production_data
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        data = []
        for row in rows:
            is_bottleneck = row['throughput_rate'] < 800 if row['throughput_rate'] else False
            
            data.append({
                'timestamp': row['timestamp'],
                'stage': row['stage'],
                'actual': round(row['actual_tonnage'], 1),
                'target': round(row['target_tonnage'], 1),
                'throughput': round(row['throughput_rate'], 1) if row['throughput_rate'] else 0,
                'efficiency': round(row['efficiency_percent'], 1) if row['efficiency_percent'] else 0,
                'shift': row['shift'],
                'supervisor': row['shift_supervisor'],
                'equipment': row['equipment_status'],
                'ore_grade': round(row['ore_grade'], 2) if row['ore_grade'] else 0,
                'weather': row['weather_condition'],
                'operator': row['operator_name'],
                'is_bottleneck': is_bottleneck
            })
        
        return jsonify({'success': True, 'data': data})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================
# API 5: Daily Summary
# ============================================================

@app.route('/api/daily_summary')
def get_daily_summary():
    """Get daily summary data"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT date, total_actual, total_target, overall_efficiency,
                   bottleneck_count, avg_throughput
            FROM daily_summary
            ORDER BY date DESC
            LIMIT 14
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        summary = []
        for row in rows:
            summary.append({
                'date': row['date'],
                'actual': round(row['total_actual'], 1),
                'target': round(row['total_target'], 1),
                'efficiency': round(row['overall_efficiency'], 1),
                'bottlenecks': row['bottleneck_count'],
                'avg_throughput': round(row['avg_throughput'], 1) if row['avg_throughput'] else 0
            })
        
        return jsonify({'success': True, 'summary': summary})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================
# API 6: Statistics Overview
# ============================================================

@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM production_data")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT stage) FROM production_data")
        total_stages = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(throughput_rate) FROM production_data")
        avg_throughput = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM bottleneck_events WHERE resolved = 0")
        active_bottlenecks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM bottleneck_events")
        total_bottlenecks = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT stage, AVG(throughput_rate) as rate
            FROM production_data
            GROUP BY stage
            ORDER BY rate DESC
            LIMIT 1
        ''')
        best = cursor.fetchone()
        
        cursor.execute('''
            SELECT shift, AVG(efficiency_percent) as eff
            FROM production_data
            GROUP BY shift
            ORDER BY eff DESC
            LIMIT 1
        ''')
        best_shift = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_records': total_records,
            'total_stages': total_stages,
            'avg_throughput': round(avg_throughput, 1) if avg_throughput else 0,
            'active_bottlenecks': active_bottlenecks,
            'total_bottlenecks': total_bottlenecks,
            'best_stage': best['stage'] if best else 'N/A',
            'best_stage_rate': round(best['rate'], 1) if best else 0,
            'best_shift': best_shift['shift'] if best_shift else 'N/A',
            'best_shift_efficiency': round(best_shift['eff'], 1) if best_shift else 0
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================
# API 7: Bottleneck History
# ============================================================

@app.route('/api/bottleneck_history')
def get_bottleneck_history():
    """Get bottleneck event history"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT stage, start_time, severity, severity_score, root_cause, corrective_action
            FROM bottleneck_events
            ORDER BY start_time DESC
            LIMIT 20
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            history.append({
                'stage': row['stage'],
                'time': row['start_time'],
                'severity': row['severity'],
                'score': round(row['severity_score'], 1),
                'cause': row['root_cause'],
                'action': row['corrective_action']
            })
        
        return jsonify({'success': True, 'history': history})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        print("\n" + "=" * 60)
        print(" ERROR: Database not found!")
        print(" Please run: python data_ingestion.py first")
        print("=" * 60 + "\n")
    else:
        print("\n" + "=" * 60)
        print(" 🚀 MINE PRODUCTION DASHBOARD SERVER")
        print("=" * 60)
        print(" 📊 Dashboard URL: http://127.0.0.1:5000")
        print(" 🔄 Auto-refresh: 30 seconds")
        print(" ⚠️  Bottleneck threshold: 80% of target")
        print("=" * 60 + "\n")
        
        app.run(debug=True, host='127.0.0.1', port=5000)