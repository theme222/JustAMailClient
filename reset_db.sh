#!/bin/bash

# Configuration
DB_FILE="$HOME/.local/share/jamc/mail.db"  # Replace with your SQLite database file path
SQL_DIR="./migrations"            # Replace with the path to your directory of .sql files

rm -f "$DB_FILE"

# 1. Check if the directory exists
if [ ! -d "$SQL_DIR" ]; then
  echo "Error: Directory '$SQL_DIR' does not exist."
  exit 1
fi

echo "Starting SQL execution on '$DB_FILE'..."

# 2. Iterate through .sql files. 
# Bash globs (/*.sql) automatically expand in alphabetical order.
for file in "$SQL_DIR"/*.sql; do
  
  # Check if the glob didn't match anything (file doesn't exist)
  if [ ! -f "$file" ]; then
    echo "No .sql files found in '$SQL_DIR'."
    break
  fi

  echo "Running: $(basename "$file")"
  
  # 3. Execute the file against the SQLite database
  sqlite3 "$DB_FILE" < "$file"
  
  # 4. Check for errors
  if [ $? -ne 0 ]; then
    echo "❌ Error executing $file. Aborting."
    exit 1
  fi

done

echo "✅ All SQL files executed successfully."