"""
Excel Preprocessing Script for PostgreSQL Insertion
Processes Excel files from a folder, applies preprocessing steps, and saves as CSV files.

Preprocessing Steps:
1. Replace up arrow (↑) with + in column names
2. Replace down arrow (↓) with - in column names
3. Replace null values with PostgreSQL-compatible values
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List, Union
import sys
import re


def preprocess_column_names(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Preprocess column names by replacing arrows with signs.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    verbose : bool, optional
        Whether to print detailed logs. Default is True.
    
    Returns:
    --------
    pd.DataFrame
        Dataframe with preprocessed column names
    """
    df_clean = df.copy()
    
    # Store original column names
    original_columns = df_clean.columns.tolist()
    
    # Create mapping for column name replacements
    column_mapping = {}
    columns_changed = []
    
    for col in original_columns:
        original_col = str(col)
        new_col = original_col
        
        # Replace up arrow (↑) with +
        if '↑' in original_col:
            new_col = new_col.replace('↑', '+')
            columns_changed.append((original_col, new_col))
        
        # Replace down arrow (↓) with -
        if '↓' in original_col:
            new_col = new_col.replace('↓', '-')
            columns_changed.append((original_col, new_col))
        
        if original_col != new_col:
            column_mapping[original_col] = new_col
    
    # Rename columns
    if column_mapping:
        df_clean = df_clean.rename(columns=column_mapping)
        if verbose:
            print("\n" + "=" * 80)
            print("STEP 1: Column Name Preprocessing")
            print("=" * 80)
            print("\nColumn names changed:")
            for old_name, new_name in columns_changed:
                print(f"  '{old_name}' → '{new_name}'")
            print(f"\n✓ Processed {len(set(columns_changed))} column name(s)")
            print("=" * 80)
    else:
        if verbose:
            print("\n" + "=" * 80)
            print("STEP 1: Column Name Preprocessing")
            print("=" * 80)
            print("  ✓ No column names contain arrows (↑ or ↓)")
            print("=" * 80)
    
    return df_clean


def preprocess_null_values(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Preprocess null values by replacing them with PostgreSQL-compatible values.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    verbose : bool, optional
        Whether to print detailed logs. Default is True.
    
    Returns:
    --------
    pd.DataFrame
        Dataframe with null values replaced
    """
    df_clean = df.copy()
    
    if verbose:
        print("\n" + "=" * 80)
        print("STEP 4: Null Value Analysis")
        print("=" * 80)
        print("\nNull value counts for each column:")
        null_counts = df_clean.isnull().sum()
        print(null_counts)
        print(f"\nTotal null values: {null_counts.sum()}")
        
        columns_with_nulls = null_counts[null_counts > 0]
        if len(columns_with_nulls) > 0:
            print("\nColumns with null values:")
            for col in columns_with_nulls.index:
                print(f"  {col}: {columns_with_nulls[col]} null value(s)")
        else:
            print("\n  ✓ No columns have null values")
        print("=" * 80)
    
    # Replace null values based on column type
    if verbose:
        print("\n" + "=" * 80)
        print("STEP 5: Replacing Null Values")
        print("=" * 80)
    
    null_replacement_summary = []
    
    for col in df_clean.columns:
        null_count = df_clean[col].isnull().sum()
        
        if null_count > 0:
            if pd.api.types.is_numeric_dtype(df_clean[col]):
                # For numeric columns, replace with 0
                df_clean[col] = df_clean[col].fillna(0)
                replacement_value = 0
            else:
                # For string/object columns, replace with "None"
                df_clean[col] = df_clean[col].fillna("None")
                replacement_value = "None"
            
            null_replacement_summary.append({
                'column': col,
                'null_count': null_count,
                'replacement': replacement_value,
                'type': 'numeric' if pd.api.types.is_numeric_dtype(df_clean[col]) else 'string'
            })
    
    if null_replacement_summary:
        if verbose:
            print("\nNull value replacements:")
            for item in null_replacement_summary:
                print(f"  Column '{item['column']}' ({item['type']}): "
                      f"Replaced {item['null_count']} null(s) with {item['replacement']}")
    else:
        if verbose:
            print("  ✓ No null values found to replace")
    
    # Verify replacement
    if verbose:
        print("\n" + "=" * 80)
        print("STEP 6: Verifying Null Value Replacement")
        print("=" * 80)
        null_counts_after = df_clean.isnull().sum()
        print("\nNull value counts after replacement:")
        print(null_counts_after)
        print(f"\nTotal null values after replacement: {null_counts_after.sum()}")
        
        if null_counts_after.sum() == 0:
            print("\n✓ All null values have been successfully replaced")
        else:
            print(f"\n⚠ Warning: {null_counts_after.sum()} null values still remain")
        print("=" * 80)
    
    return df_clean


def preprocess_arrow_values(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Preprocess data values containing arrows by converting them to numeric values.
    - Up arrow (↑) followed by number → positive number (e.g., "↑ 7" → 7)
    - Down arrow (↓) followed by number → negative number (e.g., "↓ 4" → -4)
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    verbose : bool, optional
        Whether to print detailed logs. Default is True.
    
    Returns:
    --------
    pd.DataFrame
        Dataframe with arrow values converted to numeric
    """
    df_clean = df.copy()
    
    if verbose:
        print("\n" + "=" * 80)
        print("STEP 2: Processing Arrow Values in Data")
        print("=" * 80)
    
    columns_processed = []
    
    for col in df_clean.columns:
        # Convert column to string to check for arrow values
        col_str = df_clean[col].astype(str).str.strip()
        
        # Check if column contains arrow values (↑ or ↓)
        has_up_arrow = col_str.str.contains('↑', regex=False, na=False).any()
        has_down_arrow = col_str.str.contains('↓', regex=False, na=False).any()
        
        if has_up_arrow or has_down_arrow:
            if verbose:
                print(f"\nProcessing column '{col}'...")
            
            # Count arrow values
            up_arrow_count = col_str.str.contains('↑', regex=False, na=False).sum()
            down_arrow_count = col_str.str.contains('↓', regex=False, na=False).sum()
            
            if verbose:
                print(f"  Found {up_arrow_count} up arrow (↑) value(s)")
                print(f"  Found {down_arrow_count} down arrow (↓) value(s)")
            
            # Function to convert arrow values to numeric
            def convert_arrow_to_numeric(value):
                try:
                    value_str = str(value).strip()
                    
                    # Handle NaN or None
                    if pd.isna(value) or value_str in ['None', 'nan', 'NaN', '']:
                        return 0
                    
                    # Check for up arrow (↑) - positive number
                    if '↑' in value_str:
                        # Extract number after arrow (e.g., "↑ 7" → 7)
                        match = re.search(r'↑\s*(\d+)', value_str)
                        if match:
                            return int(match.group(1))
                        return 0
                    
                    # Check for down arrow (↓) - negative number
                    elif '↓' in value_str:
                        # Extract number after arrow (e.g., "↓ 4" → -4)
                        match = re.search(r'↓\s*(\d+)', value_str)
                        if match:
                            return -int(match.group(1))  # Negative number
                        return 0
                    
                    # If no arrow, try to convert to numeric directly
                    else:
                        # Try to convert to numeric
                        try:
                            return float(value_str)
                        except (ValueError, TypeError):
                            return 0
                            
                except Exception:
                    return 0
            
            # Apply conversion
            original_dtype = df_clean[col].dtype
            df_clean[col] = df_clean[col].apply(convert_arrow_to_numeric)
            
            # Convert to numeric type
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
            
            # Convert to int if all values are integers
            if df_clean[col].dtype == 'float64':
                if (df_clean[col] % 1 == 0).all():
                    df_clean[col] = df_clean[col].astype('int64')
            
            columns_processed.append({
                'column': col,
                'original_type': str(original_dtype),
                'new_type': str(df_clean[col].dtype),
                'up_arrow_count': up_arrow_count,
                'down_arrow_count': down_arrow_count
            })
            
            if verbose:
                print(f"  ✓ Converted column '{col}' from {original_dtype} to {df_clean[col].dtype}")
                print(f"  Sample values after conversion:")
                sample_values = df_clean[col].head(5)
                for idx, val in enumerate(sample_values):
                    print(f"    Row {idx}: {val}")
    
    if not columns_processed:
        if verbose:
            print("  ✓ No columns contain arrow values (↑ or ↓)")
    else:
        if verbose:
            print(f"\n✓ Processed {len(columns_processed)} column(s) with arrow values")
    
    if verbose:
        print("=" * 80)
    
    return df_clean


def preprocess_dataframe(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Apply all preprocessing steps to the dataframe.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    verbose : bool, optional
        Whether to print detailed logs. Default is True.
    
    Returns:
    --------
    pd.DataFrame
        Preprocessed dataframe
    """
    df_clean = df.copy()
    
    # Step 1: Preprocess column names (replace arrows)
    df_clean = preprocess_column_names(df_clean, verbose=verbose)
    
    # Step 2: Preprocess arrow values in data (convert ↑ to positive, ↓ to negative numbers)
    df_clean = preprocess_arrow_values(df_clean, verbose=verbose)
    
    # Step 3: Preprocess null values
    df_clean = preprocess_null_values(df_clean, verbose=verbose)
    
    # Convert all column names to strings to ensure proper CSV saving
    df_clean.columns = [str(col) for col in df_clean.columns]
    
    if verbose:
        print("\n" + "=" * 80)
        print("Preprocessing Complete!")
        print("=" * 80)
        print(f"\nFinal shape: {df_clean.shape[0]} rows × {df_clean.shape[1]} columns")
        print(f"Final columns: {list(df_clean.columns)}")
        print("=" * 80)
    
    return df_clean


def process_excel_file(
    excel_file_path: Union[str, Path],
    output_dir: Union[str, Path],
    sheet_name: Optional[Union[str, int]] = 0,
    header_row: int = 1,
    verbose: bool = True
) -> Path:
    """
    Process a single Excel file and save as preprocessed CSV file.
    
    Parameters:
    -----------
    excel_file_path : str | Path
        Path to the input Excel file (.xlsx or .xls)
    output_dir : str | Path
        Directory to save output CSV file
    sheet_name : str | int | None, optional
        Sheet name or index to read. Default is 0 (first sheet).
    header_row : int, optional
        Row number to use as header (0-indexed). Default is 1.
    verbose : bool, optional
        Whether to print detailed preprocessing logs. Default is True.
    
    Returns:
    --------
    Path
        Path to the output .csv file
    
    Raises:
    -------
    FileNotFoundError
        If input Excel file doesn't exist
    Exception
        If processing fails
    """
    # Convert to Path objects
    excel_path = Path(excel_file_path)
    output_dir_path = Path(output_dir)
    
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")
    
    if not excel_path.suffix.lower() in ['.xlsx', '.xls']:
        raise ValueError(f"File must be an Excel file (.xlsx or .xls), got: {excel_path.suffix}")
    
    # Create output directory if it doesn't exist
    output_dir_path.mkdir(parents=True, exist_ok=True)
    
    # Generate output file path
    output_path = output_dir_path / f"{excel_path.stem}.csv"
    
    if verbose:
        print(f"\n{'='*80}")
        print(f"📊 Processing Excel File: {excel_path.name}")
        print(f"{'='*80}")
        print(f"   Input: {excel_path}")
        print(f"   Output: {output_path}")
    
    try:
        # Read Excel file
        # First, read row 0 to get proper column names
        if verbose:
            print(f"\n   Reading Excel file (sheet: {sheet_name})...")
            print(f"   Reading column names from row 0...")
        
        # Read row 0 to get column names
        df_headers = pd.read_excel(excel_path, sheet_name=sheet_name, header=0, nrows=0)
        column_names = df_headers.columns.tolist()
        
        # Now read the data starting from header_row (skip header_row rows)
        if verbose:
            print(f"   Reading data starting from row {header_row + 1}...")
        
        # Read the full file without header, then assign proper column names
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
        
        # Skip rows up to header_row and use row 0 column names
        if header_row >= 0:
            df = df.iloc[header_row + 1:].reset_index(drop=True)
            df.columns = column_names
        
        if verbose:
            print(f"   Original shape: {df.shape[0]} rows × {df.shape[1]} columns")
            print(f"   Original columns: {list(df.columns)}")
        
        # Preprocess dataframe
        if verbose:
            print("\n   Applying preprocessing steps...")
        
        df_clean = preprocess_dataframe(df, verbose=verbose)
        
        # Save to CSV
        df_clean.to_csv(output_path, index=False, encoding='utf-8')
        
        if verbose:
            print(f"\n✅ CSV saved → {output_path}")
            print(f"\n--- CSV PREVIEW (first 5 rows) ---")
            print(df_clean.head().to_string())
            print("-----------------------------------\n")
        
        return output_path
        
    except Exception as e:
        error_msg = f"❌ Error processing {excel_path.name}: {str(e)}"
        if verbose:
            print(error_msg)
        raise Exception(error_msg) from e


def process_excel_folder(
    input_folder_path: Union[str, Path],
    output_folder_name: str = "preprocessing",
    sheet_name: Optional[Union[str, int]] = 0,
    header_row: int = 0,
    verbose: bool = True
) -> List[Path]:
    """
    Process all Excel files in a folder and save preprocessed CSV files.
    
    Parameters:
    -----------
    input_folder_path : str | Path
        Path to the folder containing Excel files
    output_folder_name : str, optional
        Name of the output folder (will be created in the same directory as input).
        Default is "preprocessing".
    sheet_name : str | int | None, optional
        Sheet name or index to read. Default is 0 (first sheet).
    header_row : int, optional
        Row number to use as header (0-indexed). Default is 1.
    verbose : bool, optional
        Whether to print detailed preprocessing logs. Default is True.
    
    Returns:
    --------
    List[Path]
        List of paths to output .csv files
    
    Raises:
    -------
    FileNotFoundError
        If input folder doesn't exist
    """
    input_folder = Path(input_folder_path)
    
    if not input_folder.exists():
        raise FileNotFoundError(f"Input folder not found: {input_folder}")
    
    if not input_folder.is_dir():
        raise ValueError(f"Path is not a directory: {input_folder}")
    
    # Create output directory (in the same parent directory as input)
    output_dir = input_folder.parent / output_folder_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all Excel files
    excel_files = list(input_folder.glob("*.xlsx")) + list(input_folder.glob("*.xls"))
    
    if not excel_files:
        if verbose:
            print(f"⚠ No Excel files found in {input_folder}")
            print(f"   Supported formats: .xlsx, .xls")
        return []
    
    if verbose:
        print(f"\n{'='*80}")
        print(f"📁 Processing Excel Files from Folder")
        print(f"{'='*80}")
        print(f"   Input folder: {input_folder}")
        print(f"   Output folder: {output_dir}")
        print(f"   Found {len(excel_files)} Excel file(s)")
        print(f"{'='*80}\n")
    
    output_paths = []
    failed_files = []
    
    for idx, excel_file in enumerate(excel_files, 1):
        try:
            if verbose:
                print(f"\n[{idx}/{len(excel_files)}] Processing: {excel_file.name}")
            
            output_path = process_excel_file(
                excel_file_path=excel_file,
                output_dir=output_dir,
                sheet_name=sheet_name,
                header_row=header_row,
                verbose=verbose
            )
            output_paths.append(output_path)
            
        except Exception as e:
            failed_files.append((excel_file.name, str(e)))
            if verbose:
                print(f"⚠ Skipping {excel_file.name}: {str(e)}")
            continue
    
    # Summary
    if verbose:
        print(f"\n{'='*80}")
        print(f"✅ Processing Complete!")
        print(f"{'='*80}")
        print(f"   Successfully processed: {len(output_paths)} file(s)")
        print(f"   Failed: {len(failed_files)} file(s)")
        print(f"   Output location: {output_dir}")
        
        if failed_files:
            print(f"\n   Failed files:")
            for file_name, error in failed_files:
                print(f"     - {file_name}: {error}")
        
        print(f"{'='*80}\n")
    
    return output_paths


def main():
    """
    Main function to run the preprocessing script.
    Can be called from command line with folder path as argument.
    """
    if len(sys.argv) > 1:
        # Get folder path from command line argument
        input_folder = sys.argv[1]
        
        # Optional: output folder name as second argument
        output_folder = sys.argv[2] if len(sys.argv) > 2 else "preprocessing"
        
        # Optional: verbose flag as third argument
        verbose = sys.argv[3].lower() != 'false' if len(sys.argv) > 3 else True
        
        try:
            process_excel_folder(
                input_folder_path=input_folder,
                output_folder_name=output_folder,
                verbose=verbose
            )
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            sys.exit(1)
    else:
        # Default: process the Data/Input folder
        script_dir = Path(__file__).parent.resolve()
        base_dir = script_dir.parent
        input_folder = base_dir / "Data" / "Input"
        
        if not input_folder.exists():
            print(f"❌ Input folder not found: {input_folder}")
            print(f"\nUsage:")
            print(f"  python {Path(__file__).name} <input_folder_path> [output_folder_name] [verbose]")
            print(f"\nExample:")
            print(f"  python {Path(__file__).name} /path/to/excel/folder preprocessing true")
            sys.exit(1)
        
        print(f"📁 Using default input folder: {input_folder}")
        process_excel_folder(
            input_folder_path=input_folder,
            output_folder_name="preprocessing",
            verbose=True
        )


if __name__ == "__main__":
    main()

