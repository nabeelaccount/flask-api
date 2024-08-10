import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask, request
from datetime import datetime
import uuid

# Define Flask endpoints and connection
load_dotenv()

# Define SQL queries
CREATE_TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID UNIQUE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
"""

INSERT_TRANSACTION_RETURN_ID = """
INSERT INTO transactions (transaction_id, amount, timestamp)
VALUES (%s, %s, %s) RETURNING id;
"""

# Initialize Flask app and database connection
app = Flask(__name__)
url = os.getenv("DATABASE_URL")
connection = psycopg2.connect(url)

@app.post("/api/transaction")
def create_transaction():
    data = request.get_json()
    
    # Generate a unique transaction_id if not provided
    transaction_id = data.get("transactionId") or str(uuid.uuid4())
    amount = data["amount"]
    timestamp = data.get("timestamp") or datetime.utcnow().isoformat()

    # Convert timestamp to a datetime object
    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_TRANSACTIONS_TABLE)
            cursor.execute(INSERT_TRANSACTION_RETURN_ID, (transaction_id, amount, timestamp))
            transaction_db_id = cursor.fetchone()[0]

    return {"id": transaction_db_id, "message": f"""Transaction {transaction_id} created.
Amount £{amount} 
Timestamp {timestamp}"""}, 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)