"""
Excel to Text Processor using LlamaParse
Takes Excel file and LlamaParser as input, outputs .txt file
"""

import os
from pathlib import Path
from llama_parse import LlamaParse
from llama_index.core.node_parser import SimpleNodeParser


def process_excel_to_text(
    excel_file_path: str | Path,
    parser: LlamaParse,
    output_dir: str | Path | None = None,
    output_file_path: str | Path | None = None
) -> Path:
    """
    Process an Excel file using LlamaParse and save as .txt file.
    
    Parameters:
    -----------
    excel_file_path : str | Path
        Path to the input Excel file (.xlsx or .xls)
    parser : LlamaParse
        Initialized LlamaParse parser instance
    output_dir : str | Path | None, optional
        Directory to save output file. If None, saves in same directory as input.
    output_file_path : str | Path | None, optional
        Full path for output file. If provided, overrides output_dir.
        If None, generates filename from input filename.
    
    Returns:
    --------
    Path
        Path to the output .txt file
    
    Raises:
    -------
    FileNotFoundError
        If input Excel file doesn't exist
    Exception
        If parsing fails
    """
    # Convert to Path objects
    excel_path = Path(excel_file_path)
    
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")
    
    if not excel_path.suffix.lower() in ['.xlsx', '.xls']:
        raise ValueError(f"File must be an Excel file (.xlsx or .xls), got: {excel_path.suffix}")
    
    # Determine output path
    if output_file_path:
        output_path = Path(output_file_path)
    elif output_dir:
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        output_path = output_dir_path / f"{excel_path.stem}.txt"
    else:
        # Save in same directory as input
        output_path = excel_path.parent / f"{excel_path.stem}.txt"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n📄 Processing: {excel_path.name}")
    print(f"   Input: {excel_path}")
    print(f"   Output: {output_path}")
    
    try:
        # Parse using LlamaParse
        parsed_docs = parser.load_data(str(excel_path))
        
        # Node parsing (chunking)
        node_parser = SimpleNodeParser()
        nodes = node_parser.get_nodes_from_documents(parsed_docs)
        
        # Combine nodes to final Markdown
        markdown_output = "\n\n".join([node.get_content() for node in nodes])
        
        # Save markdown file
        output_path.write_text(markdown_output, encoding="utf-8")
        
        print(f"✅ Output saved → {output_path}")
        
        # Print preview
        print("\n--- MARKDOWN OUTPUT PREVIEW ---")
        print(markdown_output[:500])
        print("\n--------------------------------")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error processing {excel_path.name}: {str(e)}")
        raise


def process_excel_files_batch(
    excel_files: list[str | Path],
    parser: LlamaParse,
    output_dir: str | Path | None = None
) -> list[Path]:
    """
    Process multiple Excel files in batch.
    
    Parameters:
    -----------
    excel_files : list[str | Path]
        List of paths to Excel files
    parser : LlamaParse
        Initialized LlamaParse parser instance
    output_dir : str | Path | None, optional
        Directory to save output files
    
    Returns:
    --------
    list[Path]
        List of paths to output .txt files
    """
    output_paths = []
    
    for excel_file in excel_files:
        try:
            output_path = process_excel_to_text(
                excel_file_path=excel_file,
                parser=parser,
                output_dir=output_dir
            )
            output_paths.append(output_path)
        except Exception as e:
            print(f"⚠ Skipping {excel_file}: {str(e)}")
            continue
    
    return output_paths


# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get API key
    LLAMA_CLOUD_API_KEY = os.getenv("LLAMA_CLOUD_API_KEY")
    
    if not LLAMA_CLOUD_API_KEY:
        raise Exception("❌ Missing LLAMA_CLOUD_API_KEY in environment variables")
    
    # Get the base directory (project root)
    # Script is in: excel_rag_pipeline_pandas/src/pre-proccessing/
    # Base dir is: excel_rag_pipeline_pandas/
    script_dir = Path(__file__).parent.resolve()
    base_dir = script_dir.parent.parent  # Go up two levels to project root
    
    # Define input and output directories
    input_dir = base_dir / "data" / "input"
    output_dir = base_dir / "data" / "output"
    
    # Check if input directory exists
    if not input_dir.exists():
        print(f"❌ Input directory not found: {input_dir}")
        print(f"   Please create the directory and add Excel files to it.")
        exit(1)
    
    # Find all Excel files in the input directory
    excel_files = list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.xls"))
    
    if not excel_files:
        print(f"⚠ No Excel files found in {input_dir}")
        print(f"   Supported formats: .xlsx, .xls")
        exit(0)
    
    print(f"📁 Found {len(excel_files)} Excel file(s) in {input_dir}")
    print(f"📁 Output directory: {output_dir}\n")
    
    # Initialize parser
    parser = LlamaParse(
        api_key=LLAMA_CLOUD_API_KEY,
        result_type="markdown",
        parsing_instruction="Extract structured Excel tables as readable markdown."
    )
    
    # Process all Excel files
    output_paths = process_excel_files_batch(
        excel_files=excel_files,
        parser=parser,
        output_dir=output_dir
    )
    
    print(f"\n{'='*60}")
    print(f"✅ Processing complete!")
    print(f"   Processed: {len(output_paths)} file(s)")
    print(f"   Output location: {output_dir}")
    print(f"{'='*60}")
