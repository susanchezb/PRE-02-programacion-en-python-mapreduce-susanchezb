"""Word count MapReduce helpers."""

import glob
import os
import string


def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder."""
    input_directory = "files/input"

    if os.path.exists(input_directory):
        for file_path in glob.glob(f"{input_directory}/*"):
            os.remove(file_path)
    else:
        os.makedirs(input_directory)

    for file_path in glob.glob("files/raw/*"):
        with open(file_path, "r", encoding="utf-8") as file_handle:
            text = file_handle.read()

        raw_filename_with_extension = os.path.basename(file_path)
        raw_filename_without_extension = os.path.splitext(raw_filename_with_extension)[0]

        for index in range(1, n + 1):
            new_filename = f"{raw_filename_without_extension}_{index}.txt"
            with open(f"{input_directory}/{new_filename}", "w", encoding="utf-8") as output_file:
                output_file.write(text)


def load_input(input_directory):
    """Load every line from the input directory."""
    sequence = []
    for file_path in glob.glob(f"{input_directory}/*"):
        with open(file_path, "r", encoding="utf-8") as file_handle:
            for line in file_handle:
                sequence.append((file_path, line))
    return sequence


def preprocess_line(line):
    """Normalize a line and split it into words."""
    line = line.lower()
    line = line.translate(str.maketrans("", "", string.punctuation))
    line = line.replace("\n", "")
    return line.split()


def map_line(line):
    """Map a line to word-count pairs."""
    return [(word, 1) for word in preprocess_line(line)]


def mapper(sequence):
    """Mapper stage."""
    pairs_sequence = []
    for _, line in sequence:
        pairs_sequence.extend(map_line(line))
    return pairs_sequence


def shuffle_and_sort(sequence):
    """Shuffle and sort stage."""
    return sorted(sequence)


def compute_sum_by_group(group):
    """Sum the values for a grouped key."""
    key = group[0][0]
    total = sum(value for _, value in group)
    return key, total


def reducer(sequence):
    """Reducer stage."""
    result = []
    for key, value in sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


def create_directory(directory):
    """Create or clean the output directory."""
    if os.path.exists(directory):
        for file_path in glob.glob(f"{directory}/*"):
            os.remove(file_path)
    else:
        os.makedirs(directory)


def save_output(output_directory, sequence):
    """Save the reducer output."""
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as output_file:
        for key, value in sequence:
            output_file.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Create the success marker file."""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as output_file:
        output_file.write("")


def run_job(input_directory, output_directory):
    """Run the word count pipeline."""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)

if __name__ == "__main__":
    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()
    run_job("files/input", "files/output")
    end_time = time.time()

    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
