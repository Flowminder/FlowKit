from pathlib import Path

def get_output_filename(input_path, output_dir, label):
    input_path = Path(input_path)
    return f"{output_dir}/{input_path.stem}_{label}{input_path.suffix}"