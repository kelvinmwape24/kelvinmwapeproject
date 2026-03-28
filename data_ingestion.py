"""
MINE PRODUCTION DASHBOARD - ADVANCED DATA INGESTION
Mulungushi University | Mwape Kelvin
Objective 1: Collect and consolidate production data from multiple sources
"""

import sqlite3
import os
import random
import json
from datetime import datetime, timedelta
import pandas as pd

print("=" * 80)
print(" 🏭 MULUNGUSHI UNIVERSITY - MINE PRODUCTION DASHBOARD")
print(" 📊 ADVANCED DATA INGESTION SYSTEM")
print("=" * 80)

# ============================================================
# STEP 1: CREATE DATABASE WITH ADVANCED STRUCTURE
# ============================================================

def create_advanced_database():
    """Create database with multiple tables for comprehensive tracking"""
    print("\n[STEP 1] CREATING ADVANCED DATABASE STRUCTURE...")
    
    os.makedirs('database', exist_ok=True)
    conn = sqlite3.connect('database/production.db')
    cursor = conn.cursor()
    
    # TABLE 1: Production Data (main production records)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS production_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            date TEXT NOT NULL,
            hour INTEGER,
            stage TEXT NOT NULL,
            stage_order INTEGER,
            actual_tonnage REAL NOT NULL,
            target_tonnage REAL NOT NULL,
            throughput_rate REAL,
            efficiency_percent REAL,
            shift TEXT,
            shift_supervisor TEXT,
            equipment_status TEXT,
            downtime_minutes INTEGER DEFAULT 0,
            ore_grade REAL,
            weather_condition TEXT,
            operator_name TEXT,
            notes TEXT
        )
    ''')
    
    # TABLE 2: Bottleneck Events (tracking production constraints)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bottleneck_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            duration_minutes INTEGER,
            severity TEXT,
            severity_score REAL,
            root_cause TEXT,
            corrective_action TEXT,
            resolved INTEGER DEFAULT 0,
            resolved_time TEXT
        )
    ''')
    
    # TABLE 3: Daily Summary (aggregated data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            total_actual REAL,
            total_target REAL,
            overall_efficiency REAL,
            bottleneck_count INTEGER,
            max_throughput REAL,
            min_throughput REAL,
            avg_throughput REAL
        )
    ''')
    
    # TABLE 4: Equipment Performance
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS equipment_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage TEXT NOT NULL,
            date TEXT NOT NULL,
            uptime_percent REAL,
            downtime_minutes INTEGER,
            production_loss REAL,
            maintenance_needed TEXT
        )
    ''')
    
    # TABLE 5: Shift Performance
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS shift_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shift TEXT NOT NULL,
            date TEXT NOT NULL,
            supervisor TEXT,
            total_production REAL,
            efficiency REAL,
            bottlenecks_handled INTEGER
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("   ✓ Database created with 5 tables:")
    print("     - production_data (main records)")
    print("     - bottleneck_events (constraint tracking)")
    print("     - daily_summary (aggregated data)")
    print("     - equipment_performance (equipment stats)")
    print("     - shift_performance (shift analytics)")

# ============================================================
# STEP 2: GENERATE REALISTIC PRODUCTION DATA
# ============================================================

def generate_advanced_production_data():
    """Generate realistic copper mine production data"""
    print("\n[STEP 2] GENERATING ADVANCED PRODUCTION DATA...")
    
    # Production stages with order
    stages = [
        {'name': 'Mining', 'order': 1, 'target': 1200, 'threshold': 960},
        {'name': 'Crushing', 'order': 2, 'target': 1150, 'threshold': 920},
        {'name': 'Milling', 'order': 3, 'target': 1100, 'threshold': 880},
        {'name': 'Flotation', 'order': 4, 'target': 1050, 'threshold': 840},
        {'name': 'Smelting', 'order': 5, 'target': 1000, 'threshold': 800}
    ]
    
    shifts = [
        {'name': 'Morning', 'hours': (6, 14), 'supervisors': ['Mr. Banda', 'Mr. Phiri']},
        {'name': 'Afternoon', 'hours': (14, 22), 'supervisors': ['Mrs. Chanda', 'Mr. Zulu']},
        {'name': 'Night', 'hours': (22, 6), 'supervisors': ['Mrs. Mwansa', 'Mr. Daka']}
    ]
    
    weather_conditions = ['Clear', 'Rainy', 'Cloudy', 'Windy', 'Storm']
    equipment_statuses = ['Optimal', 'Normal', 'Reduced', 'Down']
    operators = ['J. Banda', 'P. Phiri', 'M. Chanda', 'L. Zulu', 'S. Mwansa', 'K. Daka', 'E. Tembo']
    ore_grades = [random.uniform(0.8, 1.2) for _ in range(10)]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # 30 days of data
    
    all_data = []
    current = start_date
    
    print("   Processing data for 30 days (720 hours)...")
    
    while current <= end_date:
        # Determine shift
        hour = current.hour
        current_shift = None
        for shift in shifts:
            if shift['hours'][0] <= hour < shift['hours'][1]:
                current_shift = shift
                break
            if shift['name'] == 'Night' and hour >= 22 or hour < 6:
                current_shift = shift
                break
        
        supervisor = random.choice(current_shift['supervisors']) if current_shift else 'Unknown'
        weather = random.choice(weather_conditions)
        ore_grade = random.choice(ore_grades)
        
        for stage in stages:
            target = stage['target']
            threshold = stage['threshold']
            
            # Base production with variations
            base_factor = random.uniform(0.92, 1.08)
            actual = target * base_factor
            
            # Weather impact
            if weather == 'Rainy':
                actual *= random.uniform(0.85, 0.95)
            elif weather == 'Storm':
                actual *= random.uniform(0.70, 0.85)
            
            # Ore grade impact
            if stage['name'] == 'Mining':
                actual *= ore_grade
            
            # Equipment status impact
            equipment = random.choice(equipment_statuses)
            downtime = 0
            if equipment == 'Reduced':
                actual *= random.uniform(0.70, 0.90)
                downtime = random.randint(15, 60)
            elif equipment == 'Down':
                actual *= random.uniform(0.30, 0.60)
                downtime = random.randint(60, 180)
            
            # Bottleneck detection (15% chance)
            is_bottleneck = random.random() < 0.15
            if is_bottleneck:
                actual = target * random.uniform(0.45, 0.75)
            
            efficiency = (actual / target * 100) if target else 0
            
            record = {
                'timestamp': current.strftime('%Y-%m-%d %H:%M:%S'),
                'date': current.strftime('%Y-%m-%d'),
                'hour': hour,
                'stage': stage['name'],
                'stage_order': stage['order'],
                'actual_tonnage': round(actual, 1),
                'target_tonnage': target,
                'throughput_rate': round(actual, 1),
                'efficiency_percent': round(efficiency, 1),
                'shift': current_shift['name'] if current_shift else 'Night',
                'shift_supervisor': supervisor,
                'equipment_status': equipment,
                'downtime_minutes': downtime,
                'ore_grade': round(ore_grade, 2),
                'weather_condition': weather,
                'operator_name': random.choice(operators),
                'notes': ''
            }
            
            # Add note for bottlenecks
            if is_bottleneck:
                record['notes'] = f"BOTTLENECK: Production at {efficiency:.0f}% of target"
            
            all_data.append(record)
        
        current += timedelta(hours=1)
    
    print(f"   ✓ Generated {len(all_data)} production records")
    return all_data

# ============================================================
# STEP 3: DETECT AND RECORD BOTTLENECKS
# ============================================================

def detect_and_record_bottlenecks(data):
    """Analyze data to detect bottlenecks"""
    print("\n[STEP 3] DETECTING BOTTLENECKS...")
    
    thresholds = {
        'Mining': 960,
        'Crushing': 920,
        'Milling': 880,
        'Flotation': 840,
        'Smelting': 800
    }
    
    bottlenecks = []
    bottleneck_count = 0
    
    for record in data:
        stage = record['stage']
        actual = record['actual_tonnage']
        threshold = thresholds.get(stage, 800)
        
        if actual < threshold:
            severity_score = (actual / threshold) * 100
            if severity_score >= 70:
                severity = 'WARNING'
            elif severity_score >= 50:
                severity = 'SEVERE'
            else:
                severity = 'CRITICAL'
            
            bottleneck_count += 1
            bottlenecks.append({
                'stage': stage,
                'start_time': record['timestamp'],
                'severity': severity,
                'severity_score': round(severity_score, 1),
                'root_cause': determine_root_cause(record),
                'corrective_action': suggest_corrective_action(record)
            })
    
    print(f"   ✓ Detected {bottleneck_count} bottleneck events")
    return bottlenecks

def determine_root_cause(record):
    """Determine likely root cause of bottleneck"""
    if record['equipment_status'] == 'Down':
        return f"Equipment failure - {record['equipment_status']}"
    elif record['equipment_status'] == 'Reduced':
        return f"Equipment degradation - {record['equipment_status']}"
    elif record['weather_condition'] in ['Rainy', 'Storm']:
        return f"Adverse weather - {record['weather_condition']}"
    elif record['ore_grade'] < 0.9:
        return f"Low ore grade - {record['ore_grade']}"
    else:
        return "Operational inefficiency - investigate process"

def suggest_corrective_action(record):
    """Suggest corrective action based on bottleneck"""
    if record['equipment_status'] == 'Down':
        return "Immediate maintenance required. Dispatch maintenance team."
    elif record['equipment_status'] == 'Reduced':
        return "Schedule maintenance. Monitor equipment performance."
    elif record['weather_condition'] in ['Rainy', 'Storm']:
        return "Weather protocols activated. Increase safety measures."
    elif record['ore_grade'] < 0.9:
        return "Review mining plan. Adjust blending strategy."
    else:
        return "Process audit required. Review SOP and operator training."

# ============================================================
# STEP 4: LOAD DATA TO DATABASE
# ============================================================

def load_data_to_database(data, bottlenecks):
    """Load all data into database"""
    print("\n[STEP 4] LOADING DATA TO DATABASE...")
    
    conn = sqlite3.connect('database/production.db')
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("DELETE FROM production_data")
    cursor.execute("DELETE FROM bottleneck_events")
    cursor.execute("DELETE FROM daily_summary")
    
    # Load production data
    count = 0
    for record in data:
        cursor.execute('''
            INSERT INTO production_data (
                timestamp, date, hour, stage, stage_order,
                actual_tonnage, target_tonnage, throughput_rate,
                efficiency_percent, shift, shift_supervisor,
                equipment_status, downtime_minutes, ore_grade,
                weather_condition, operator_name, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record['timestamp'], record['date'], record['hour'],
            record['stage'], record['stage_order'],
            record['actual_tonnage'], record['target_tonnage'],
            record['throughput_rate'], record['efficiency_percent'],
            record['shift'], record['shift_supervisor'],
            record['equipment_status'], record['downtime_minutes'],
            record['ore_grade'], record['weather_condition'],
            record['operator_name'], record['notes']
        ))
        count += 1
        if count % 1000 == 0:
            print(f"   Loaded {count} records...")
    
    # Load bottleneck events
    for bottleneck in bottlenecks:
        cursor.execute('''
            INSERT INTO bottleneck_events (
                stage, start_time, severity, severity_score,
                root_cause, corrective_action, resolved
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            bottleneck['stage'], bottleneck['start_time'],
            bottleneck['severity'], bottleneck['severity_score'],
            bottleneck['root_cause'], bottleneck['corrective_action'], 0
        ))
    
    conn.commit()
    conn.close()
    
    print(f"   ✓ Loaded {count} production records")
    print(f"   ✓ Loaded {len(bottlenecks)} bottleneck events")

# ============================================================
# STEP 5: CALCULATE DAILY SUMMARIES
# ============================================================

def calculate_daily_summaries():
    """Calculate daily summary statistics"""
    print("\n[STEP 5] CALCULATING DAILY SUMMARIES...")
    
    conn = sqlite3.connect('database/production.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT date, 
               SUM(actual_tonnage) as total_actual,
               SUM(target_tonnage) as total_target,
               AVG(throughput_rate) as avg_throughput,
               MAX(throughput_rate) as max_throughput,
               MIN(throughput_rate) as min_throughput
        FROM production_data
        GROUP BY date
        ORDER BY date DESC
    ''')
    
    daily_data = cursor.fetchall()
    
    for day in daily_data:
        efficiency = (day[1] / day[2] * 100) if day[2] else 0
        
        # Count bottlenecks for this day
        cursor.execute('''
            SELECT COUNT(*) FROM bottleneck_events
            WHERE date(start_time) = ?
        ''', (day[0],))
        bottleneck_count = cursor.fetchone()[0]
        
        cursor.execute('''
            INSERT INTO daily_summary (
                date, total_actual, total_target, overall_efficiency,
                bottleneck_count, max_throughput, min_throughput, avg_throughput
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (day[0], day[1], day[2], efficiency, bottleneck_count,
              day[4], day[5], day[3]))
    
    conn.commit()
    conn.close()
    
    print(f"   ✓ Calculated summaries for {len(daily_data)} days")

# ============================================================
# STEP 6: DISPLAY COMPREHENSIVE STATISTICS
# ============================================================

def display_advanced_statistics():
    """Display comprehensive production statistics"""
    print("\n" + "=" * 80)
    print(" 📊 PRODUCTION STATISTICS DASHBOARD")
    print("=" * 80)
    
    conn = sqlite3.connect('database/production.db')
    cursor = conn.cursor()
    
    # Overall statistics
    cursor.execute("SELECT COUNT(*) FROM production_data")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(actual_tonnage), SUM(target_tonnage) FROM production_data")
    total_actual, total_target = cursor.fetchone()
    
    overall_efficiency = (total_actual / total_target * 100) if total_target else 0
    
    print(f"\n 📈 OVERALL STATISTICS")
    print(f"    Total Records: {total_records:,}")
    print(f"    Total Production: {total_actual:,.0f} tons")
    print(f"    Total Target: {total_target:,.0f} tons")
    print(f"    Overall Efficiency: {overall_efficiency:.1f}%")
    
    # Stage-wise statistics
    print(f"\n 🏭 STAGE-WISE STATISTICS")
    print("    " + "-" * 75)
    print("    Stage        | Actual (tons) | Target (tons) | Efficiency | Avg Throughput")
    print("    " + "-" * 75)
    
    cursor.execute('''
        SELECT stage, SUM(actual_tonnage), SUM(target_tonnage), AVG(throughput_rate)
        FROM production_data
        GROUP BY stage
        ORDER BY stage_order
    ''')
    
    for row in cursor.fetchall():
        eff = (row[1] / row[2] * 100) if row[2] else 0
        print(f"    {row[0]:12} | {row[1]:12,.0f} | {row[2]:12,.0f} | {eff:8.1f}% | {row[3]:8.1f} t/h")
    
    # Bottleneck statistics
    cursor.execute("SELECT COUNT(*) FROM bottleneck_events")
    total_bottlenecks = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT severity, COUNT(*) FROM bottleneck_events
        GROUP BY severity
    ''')
    
    print(f"\n ⚠️ BOTTLENECK STATISTICS")
    print(f"    Total Bottlenecks: {total_bottlenecks}")
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]}")
    
    # Shift performance
    print(f"\n 👥 SHIFT PERFORMANCE")
    print("    " + "-" * 65)
    print("    Shift        | Total Production | Avg Efficiency | Bottlenecks Handled")
    print("    " + "-" * 65)
    
    cursor.execute('''
        SELECT shift, SUM(actual_tonnage), AVG(efficiency_percent), COUNT(DISTINCT id)
        FROM production_data
        GROUP BY shift
    ''')
    
    for row in cursor.fetchall():
        print(f"    {row[0]:12} | {row[1]:15,.0f} | {row[2]:12.1f}% | {row[3]:18}")
    
    conn.close()

# ============================================================
# MAIN FUNCTION
# ============================================================

def main():
    print("\n" + "=" * 80)
    print(" 🚀 STARTING ADVANCED DATA INGESTION PROCESS")
    print("=" * 80)
    
    create_advanced_database()
    production_data = generate_advanced_production_data()
    bottlenecks = detect_and_record_bottlenecks(production_data)
    load_data_to_database(production_data, bottlenecks)
    calculate_daily_summaries()
    display_advanced_statistics()
    
    print("\n" + "=" * 80)
    print(" ✅ OBJECTIVE 1 COMPLETE!")
    print(" 📁 Database saved to: database/production.db")
    print(" 🚀 Next step: Run 'python app.py' to start dashboard")
    print("=" * 80)

if __name__ == '__main__':
    main()