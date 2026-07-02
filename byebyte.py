import json
import sys

def main():
    # If a file path is passed as an argument, read that file.
    # Otherwise, accept input directly from the terminal (stdin).
    try:
        if len(sys.argv) > 1:
            with open(sys.argv[1], 'r') as f:
                content = f.read()
        else:
            print("Paste your JSON array here (Press Ctrl+D on Linux/Mac or Ctrl+Z on Windows to finish):", file=sys.stderr)
            content = sys.stdin.read()

        if not content.strip():
            return

        # Parse the JSON string into a Python list
        byte_list = json.loads(content)

        # Ensure it's a list
        if not isinstance(byte_list, list):
            raise ValueError("Provided JSON is not an array.")

        # Convert the list of integers into a true byte array, 
        # then decode to a string. 'replace' ignores fatal UTF-8 errors.
        decoded_string = bytes(byte_list).decode('utf-8', errors='replace')
        
        print("\n--- DECODED OUTPUT ---")
        print(decoded_string)
        print("----------------------\n")
        
    except json.JSONDecodeError:
        print("Error: Invalid JSON format. Make sure it looks like [104, 101, 108...]", file=sys.stderr)
    except ValueError as e:
        print(f"Error: {e}. The array must contain integers between 0 and 255.", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()