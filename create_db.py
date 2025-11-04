import sqlite3
import datetime

# --- Data to Insert ---

# 1. User data
# (user_id is AUTOINCREMENT)
users_data = [
    ("alice_dev", "alice@example.com"),
    ("bob_data", "bob@datasci.com"),
    ("charlie_ops", None),  # <-- Email is NULL
    ("david_pm", "david@product.org"),
    ("erin_analytics", "erin@analysis.co"),
]

# 2. Product data
# (product_id is AUTOINCREMENT)
products_data = [
    # (product_name, price, seller_id)
    ("Quantum Laptop", 1499.99, 1),
    ("AI-Powered Mouse", 89.50, 2),
    ("Ergonomic Keyboard", 175.00, 1),
    ("4K Monitor (32-inch)", 650.00, 4),
    ("Data-Scribe Pro License", 99.00, 5),
    ("Cloud Credits (100)", 100.00, 3),
    ("Mini Server Rack", 320.00, 2),
    ("VR Headset (Dev Kit)", None, 4),  # <-- Price is NULL
    ("SQL Pocket Guide", 25.99, 5),
    ("Legacy Mainframe Access", 9999.00, None),  # <-- Seller is NULL
]


# --- Database Script ---


def populate_database(db_path="test.db"):
    print(f"Connecting to '{db_path}' to populate data...")

    try:
        # Use 'with' statement for automatic connection management
        with sqlite3.connect(db_path) as connection:
            cursor = connection.cursor()

            # --- 1. Create Tables (IF NOT EXISTS) ---
            print("Creating 'users' table (if it doesn't exist)...")
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            )

            print("Creating 'products' table (if it doesn't exist)...")
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS products (
                product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL,
                price REAL,
                seller_id INTEGER,
                FOREIGN KEY(seller_id) REFERENCES users(user_id)
            );
            """
            )

            # --- 2. Insert Data ---
            # 'INSERT OR IGNORE' prevents crashes if you run the script
            # multiple times (due to the UNIQUE constraint on username)

            print(f"Inserting {len(users_data)} users...")
            cursor.executemany(
                "INSERT OR IGNORE INTO users (username, email) VALUES (?, ?)",
                users_data,
            )

            print(f"Inserting {len(products_data)} products...")
            cursor.executemany(
                "INSERT OR IGNORE INTO products (product_name, price, seller_id) VALUES (?, ?, ?)",
                products_data,
            )

            # --- 3. Commit ---
            # 'with' statement automatically commits on success
            print("Data insertion complete. Committing changes.")

        print(f"\n✅ Success! '{db_path}' is populated.")

    except sqlite3.Error as e:
        print(f"\n❌ ERROR: Database operation failed: {e}", exc_info=True)
    except Exception as e:
        print(f"\n❌ ERROR: An unexpected error occurred: {e}", exc_info=True)


# --- Run the script ---
if __name__ == "__main__":
    populate_database()
