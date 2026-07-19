import sqlite3
import json
import uuid
import datetime

def send_test_order():
    db_path = 'state/queue/queue.db'
    
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # Construct a valid ApprovedOrder payload
    msg = {
        'schema_version': '1',
        'message_id': str(uuid.uuid4()),
        'idempotency_key': str(uuid.uuid4()),
        'correlation_id': 'manual-test-123',
        'instrument': 'EUR_USD',
        'side': 'BUY',
        'units': 1,
        'granularity': 'H1',
        'risk_context': {
            'atr': 0.005 # 50 pips ATR
        },
        'created_at': now.isoformat().replace('+00:00', 'Z')
    }
    
    # Connect to the local queue database
    try:
        conn = sqlite3.connect(db_path)
        
        # Insert the message into AMS_Outbound_Queue
        conn.execute(
            'INSERT INTO queue (topic, body, state, attempts, available_at) VALUES (?, ?, ?, ?, ?)',
            ('AMS_Outbound_Queue', json.dumps(msg), 'ready', 0, 0.0)
        )
        conn.commit()
        conn.close()
        
        print(f"✅ Test order sent to AMS_Outbound_Queue!")
        print(f"Details: {msg['side']} {msg['units']} {msg['instrument']}")
        print("Check your System 2 terminal to see the execution log, or refresh your OANDA dashboard.")
        
    except Exception as e:
        print(f"❌ Failed to send order: {e}")
        print("Make sure you are running this script from the system-2-execution-engine directory.")

if __name__ == '__main__':
    send_test_order()
