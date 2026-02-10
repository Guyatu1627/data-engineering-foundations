# -------------------------------
# Day 1: Text File Analyzer
# -------------------------------

# Function 1: Read text from a file
def read_file(file_path):
    """
    Reads a text file and returns its content as a string.
    """
    with open(file_path, "r") as file:
        content = file.read()
    return content


# Function 2: Analyze the text
def analyze_text(text):
    """
    Analyzes text and returns statistics:
    - number of non-empty lines
    - number of words
    - number of characters (excluding spaces)
    """

    # Split text into lines
    lines = text.split("\n")

    # Count non-empty lines
    non_empty_lines = [line for line in lines if line.strip() != ""]
    line_count = len(non_empty_lines)

    # Split text into words
    words = text.split()
    word_count = len(words)

    # Count characters excluding spaces
    characters = [char for char in text if char != " "]
    char_count = len(characters)

    # Store results in a dictionary
    result = {
        "lines": line_count,
        "words": word_count,
        "characters": char_count
    }

    return result


# Function 3: Write the analysis report
def write_report(result, output_path):
    """
    Writes the analysis result into a report file.
    """
    with open(output_path, "w") as file:
        file.write("Text File Analysis Report\n")
        file.write("--------------------------\n")
        file.write(f"Total lines (non-empty): {result['lines']}\n")
        file.write(f"Total words: {result['words']}\n")
        file.write(f"Total characters (no spaces): {result['characters']}\n")


# -------------------------------
# Main execution (pipeline)
# -------------------------------

input_file_path = "input.txt"
output_file_path = "report.txt"

text = read_file(input_file_path)
analysis_result = analyze_text(text)
write_report(analysis_result, output_file_path)

print("Analysis complete. Report saved to report.txt")
