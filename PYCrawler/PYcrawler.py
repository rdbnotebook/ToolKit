import os
from pathlib import Path

def crawl_directory(root_dir: Path, output_file_path: Path) -> None:
    """
    Crawls a directory, maps its structure for .py files,
    and dumps the structure and file contents into an output file.
    """
    with open(output_file_path, "w", encoding="utf-8") as outfile:
        # First, map the directory structure for .py files
        outfile.write("Directory Structure (Python files):\n")
        outfile.write("===================================\n")
        for current_path, _, files in os.walk(root_dir):
            current_path_obj = Path(current_path)
            # Indentation based on depth relative to root_dir
            try:
                depth = len(current_path_obj.relative_to(root_dir).parts)
            except ValueError: # current_path_obj is root_dir itself
                depth = 0

            indent = "    " * depth
            # Only list directory if it's not the root or if it contains .py files directly or in subdirs
            python_files_in_current_or_subdirs = any(
                f.endswith(".py")
                for _, _, f_list in os.walk(current_path_obj)
                for f in f_list
            )

            if python_files_in_current_or_subdirs:
                # We only want to print the directory name if it contains .py files or subdirectories that do
                # For the root directory, always print its name
                if depth == 0:
                    outfile.write(f"{root_dir.name}\n")
                else:
                    # Check if this specific directory contains python files
                    # or if it's a parent of a directory that contains python files
                    py_files_directly_in_dir = [f for f in files if f.endswith(".py")]
                    if py_files_directly_in_dir or any(
                        any(sf.endswith(".py") for sf in s_files)
                        for _, s_dirs, s_files in os.walk(current_path_obj)
                        if Path(os.path.join(current_path_obj, s_dirs[0] if s_dirs else ".")).relative_to(current_path_obj).parts # check subdirs
                    ): # ensure we are looking at subdirectories
                         outfile.write(f"{indent}└── {current_path_obj.name}\n")


            for file_name in sorted(files):
                if file_name.endswith(".py"):
                    outfile.write(f"{indent}    ├── {file_name}\n")
        outfile.write("\n\n")

        # Second, dump the contents of each .py file
        outfile.write("Python File Contents:\n")
        outfile.write("=====================\n")
        for current_path, _, files in os.walk(root_dir):
            for file_name in sorted(files):
                if file_name.endswith(".py"):
                    file_path = Path(current_path) / file_name
                    relative_file_path = file_path.relative_to(root_dir)
                    header = f"--- File: {root_dir.name}/{relative_file_path} ---"
                    outfile.write(f"\n{header}\n")
                    try:
                        with open(file_path, "r", encoding="utf-8") as infile:
                            outfile.write(infile.read())
                        outfile.write(f"\n--- End of File: {root_dir.name}/{relative_file_path} ---\n")
                    except Exception as e:
                        outfile.write(f"Error reading file {file_path}: {e}\n")
                        outfile.write(f"--- End of File (Error): {root_dir.name}/{relative_file_path} ---\n")

if __name__ == "__main__":
    # The script will run in the directory it's placed in, or a specified one.
    # For this implementation, it defaults to the current working directory.
    current_directory = Path.cwd()
    output_file = current_directory / "crawl_dump.txt"
    print(f"Crawling directory: {current_directory}")
    print(f"Output will be saved to: {output_file}")
    crawl_directory(current_directory, output_file)
    print("Crawling complete.") 