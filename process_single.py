import sys
import shutil

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: process_single.py <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        # Just copy the file back unchanged
        shutil.copyfile(input_file, output_file)
        print("File returned successfully:", output_file)
    except Exception as e:
        print("Error:", str(e), file=sys.stderr)
        sys.exit(1)
