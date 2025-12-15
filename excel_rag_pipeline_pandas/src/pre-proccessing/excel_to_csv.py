"""
Excel to CSV Processor with Preprocessing
Takes Excel file as input, applies preprocessing steps, outputs .csv file
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List


def preprocess_dataframe(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Preprocess dataframe with common cleaning steps.
    Includes detailed logging similar to notebook preprocessing steps.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    verbose : bool, optional
        Whether to print detailed preprocessing logs. Default is True.
    
    Returns:
    --------
    pd.DataFrame
        Preprocessed dataframe
    """
    df_clean = df.copy()
    
    if verbose:
        print("=" * 80)
        print("STEP 1: Checking Column Data Types")
        print("=" * 80)
        print("\nData Types for each column:")
        print(df_clean.dtypes)
        print("\n" + "-" * 80)
    
    # Step 1: Remove unwanted columns (Month, Sr. No., #)
    columns_to_remove = ['Month', 'Sr. No.', '#']
    removed_cols = [col for col in columns_to_remove if col in df_clean.columns]
    if removed_cols and verbose:
        print(f"\nRemoving columns: {removed_cols}")
    df_clean = df_clean.drop(
        columns=removed_cols,
        errors='ignore'
    )
    
    if verbose:
        print("\nSTEP 2: Checking Null Values in Each Column")
        print("=" * 80)
        print("\nNull value counts for each column:")
        null_counts = df_clean.isnull().sum()
        print(null_counts)
        print("\nTotal null values:", null_counts.sum())
        print("\nColumns with null values:")
        columns_with_nulls = null_counts[null_counts > 0]
        if len(columns_with_nulls) > 0:
            print(columns_with_nulls)
            for col in columns_with_nulls.index:
                print(f"\n  {col}: {columns_with_nulls[col]} null values")
        else:
            print("  No columns have null values")
        print("\n" + "-" * 80)
    
    # Step 2: Replace null values with "None"
    if verbose:
        print("\nSTEP 3: Replacing Null Values with 'None'")
        print("=" * 80)
    df_clean = df_clean.fillna("None")
    
    if verbose:
        print("\nSTEP 4: Verifying Null Values After Replacement")
        print("=" * 80)
        null_counts_after = df_clean.isnull().sum()
        print("\nNull value counts after replacement:")
        print(null_counts_after)
        print("\nTotal null values after replacement:", null_counts_after.sum())
        
        if null_counts_after.sum() == 0:
            print("\n✓ All null values have been successfully replaced with 'None'")
        else:
            print(f"\n⚠ Warning: {null_counts_after.sum()} null values still remain")
        
        print("\n" + "=" * 80)
    
    # Step 3: Replace "-" based on column type
    if verbose:
        print("\nSTEP 5: Replacing '-' Based on Column Type")
        print("=" * 80)
    
    cols_with_dash = []
    for col in df_clean.columns:
        # Convert to string to check for "-" values (handles both numeric and object types)
        col_str = df_clean[col].astype(str).str.strip()
        # Check if column contains standalone "-" (not part of negative numbers like "-123")
        # We want to match exactly "-" not "-123" or other negative numbers
        dash_mask = (col_str == '-')
        dash_count = dash_mask.sum()
        
        if dash_count > 0:
            cols_with_dash.append(col)
            # Check column type
            if pd.api.types.is_numeric_dtype(df_clean[col]):
                # For numeric/integer columns, replace "-" with 0
                df_clean.loc[dash_mask, col] = 0
                if verbose:
                    print(f"Column '{col}' (numeric): Replaced {dash_count} '-' value(s) with 0")
            else:
                # For string/object columns, replace "-" with "None"
                df_clean.loc[dash_mask, col] = 'None'
                if verbose:
                    print(f"Column '{col}' (string/object): Replaced {dash_count} '-' value(s) with 'None'")
    
    if not cols_with_dash and verbose:
        print("  ✓ No columns contain '-' values")
    
    if verbose:
        print("=" * 80)
    
    # Step 4: Process "Change +/-" column if it exists
    if 'Change +/-' in df_clean.columns:
        if verbose:
            print("\n" + "=" * 80)
            print("Processing 'Change +/-' Column")
            print("=" * 80)
            print(f"\nCurrent data type of 'Change +/-' column: {df_clean['Change +/-'].dtype}")
            print(f"\nSample values before processing:")
            print(df_clean['Change +/-'].head(10))
        
        # Replace up arrow (⬆) with + and down arrow (⬇) with -
        df_clean['Change +/-'] = df_clean['Change +/-'].astype(str).str.replace('⬆', '+', regex=False)
        df_clean['Change +/-'] = df_clean['Change +/-'].str.replace('⬇', '-', regex=False)
        
        # Handle "None" values (from previous preprocessing) - convert to 0
        df_clean['Change +/-'] = df_clean['Change +/-'].replace('None', '0')
        
        # Convert to integer
        def convert_to_int(value):
            try:
                # Remove any whitespace and convert
                value_str = str(value).strip()
                # If it's just a sign without number, return 0
                if value_str in ['+', '-', '']:
                    return 0
                return int(value_str)
            except (ValueError, TypeError):
                return 0
        
        df_clean['Change +/-'] = df_clean['Change +/-'].apply(convert_to_int)
        
        # Convert column to int dtype
        df_clean['Change +/-'] = df_clean['Change +/-'].astype('int64')
        
        if verbose:
            print(f"\nNew data type of 'Change +/-' column: {df_clean['Change +/-'].dtype}")
            print(f"\nSample values after processing:")
            print(df_clean['Change +/-'].head(10))
            print(f"\n✓ Column 'Change +/-' has been converted to integer type")
            print("=" * 80)
    
    # Step 5: Convert string numbers with commas to numeric (for all columns)
    if verbose:
        print("\nSTEP 6: Converting String Numbers with Commas to Numeric")
        print("=" * 80)
    
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            # Try to detect if it's a numeric column with commas
            sample = df_clean[col].dropna().head(10)
            if len(sample) > 0:
                try:
                    test_vals = sample.astype(str).str.replace(',', '').str.strip()
                    # Check if they're numeric (excluding "None")
                    numeric_count = test_vals[test_vals != 'None'].str.match(r'^-?\d+\.?\d*$').sum()
                    if numeric_count >= len(sample[sample.astype(str) != 'None']) * 0.8:  # 80% are numeric
                        df_clean[col] = pd.to_numeric(
                            df_clean[col].astype(str).str.replace(',', '').str.strip().replace('None', ''),
                            errors='coerce'
                        )
                        # Fill NaN back with "None" if needed
                        df_clean[col] = df_clean[col].fillna("None")
                        if verbose:
                            print(f"  ℹ Converted '{col}' from string to numeric")
                except Exception:
                    pass
    
    if verbose:
        print("=" * 80)
    
    # Step 6: Remove columns with 90%+ missing data (0 or "None")
    if verbose:
        print("\nSTEP 7: Removing Columns with 90%+ Missing Data")
        print("=" * 80)
    
    total_rows = len(df_clean)
    threshold = 0.90  # 90% threshold
    columns_to_drop = []
    
    for col in df_clean.columns:
        # Count missing values: 0 for numeric columns, "None" for object columns
        if pd.api.types.is_numeric_dtype(df_clean[col]):
            # For numeric columns, count 0 values
            missing_count = (df_clean[col] == 0).sum()
        else:
            # For object/string columns, count "None" values
            missing_count = (df_clean[col].astype(str) == 'None').sum()
        
        missing_percentage = missing_count / total_rows if total_rows > 0 else 0
        
        if missing_percentage >= threshold:
            columns_to_drop.append(col)
            if verbose:
                print(f"  ⚠ Column '{col}': {missing_percentage*100:.1f}% missing ({missing_count}/{total_rows} rows) - Will be removed")
    
    if columns_to_drop:
        df_clean = df_clean.drop(columns=columns_to_drop)
        if verbose:
            print(f"\n  ✓ Removed {len(columns_to_drop)} column(s): {columns_to_drop}")
    else:
        if verbose:
            print("  ✓ No columns with 90%+ missing data found")
    
    if verbose:
        print("=" * 80)
        print("Preprocessing Complete!")
        print("=" * 80)
    
    return df_clean


def process_excel_to_csv(
    excel_file_path: str | Path,
    output_dir: str | Path | None = None,
    output_file_path: str | Path | None = None,
    sheet_name: Optional[str | int] = 0,
    header_row: int = 1,
    columns_to_remove: Optional[List[str]] = None,
    verbose: bool = True
) -> Path:
    """
    Process an Excel file and save as preprocessed CSV file.
    
    Parameters:
    -----------
    excel_file_path : str | Path
        Path to the input Excel file (.xlsx or .xls)
    output_dir : str | Path | None, optional
        Directory to save output file. If None, saves in same directory as input.
    output_file_path : str | Path | None, optional
        Full path for output file. If provided, overrides output_dir.
        If None, generates filename from input filename.
    sheet_name : str | int | None, optional
        Sheet name or index to read. Default is 0 (first sheet).
    header_row : int, optional
        Row number to use as header (0-indexed). Default is 1.
    columns_to_remove : list[str] | None, optional
        Additional columns to remove. Default is ['Month', 'Sr. No.', '#'].
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
        output_path = output_dir_path / f"{excel_path.stem}.csv"
    else:
        # Save in same directory as input
        output_path = excel_path.parent / f"{excel_path.stem}_processed.csv"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n📊 Processing Excel to CSV: {excel_path.name}")
    print(f"   Input: {excel_path}")
    print(f"   Output: {output_path}")
    
    try:
        # Read Excel file
        print(f"   Reading Excel file (sheet: {sheet_name}, header row: {header_row})...")
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header_row)
        
        print(f"   Original shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"   Columns: {list(df.columns)}")
        
        # Preprocess dataframe
        print("   Preprocessing dataframe...")
        
        # Override default columns to remove if provided
        if columns_to_remove is not None:
            original_columns_to_remove = ['Month', 'Sr. No.', '#']
            df_clean = df.drop(
                columns=[col for col in columns_to_remove if col in df.columns],
                errors='ignore'
            )
        else:
            df_clean = df.copy()
        
        # Apply preprocessing with detailed logging
        df_clean = preprocess_dataframe(df_clean, verbose=verbose)
        
        print(f"\n   Processed shape: {df_clean.shape[0]} rows × {df_clean.shape[1]} columns")
        
        # Save to CSV
        df_clean.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"✅ CSV saved → {output_path}")
        
        # Print preview
        print("\n--- CSV PREVIEW (first 5 rows) ---")
        print(df_clean.head().to_string())
        print("\n-----------------------------------")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error processing {excel_path.name}: {str(e)}")
        raise


def process_excel_files_batch(
    excel_files: list[str | Path],
    output_dir: str | Path | None = None,
    sheet_name: Optional[str | int] = 0,
    header_row: int = 1,
    verbose: bool = True
) -> list[Path]:
    """
    Process multiple Excel files in batch.
    
    Parameters:
    -----------
    excel_files : list[str | Path]
        List of paths to Excel files
    output_dir : str | Path | None, optional
        Directory to save output files
    sheet_name : str | int | None, optional
        Sheet name or index to read
    header_row : int, optional
        Row number to use as header (0-indexed)
    verbose : bool, optional
        Whether to print detailed preprocessing logs. Default is True.
    
    Returns:
    --------
    list[Path]
        List of paths to output .csv files
    """
    output_paths = []
    
    for excel_file in excel_files:
        try:
            output_path = process_excel_to_csv(
                excel_file_path=excel_file,
                output_dir=output_dir,
                sheet_name=sheet_name,
                header_row=header_row,
                verbose=verbose
            )
            output_paths.append(output_path)
        except Exception as e:
            print(f"⚠ Skipping {excel_file}: {str(e)}")
            continue
    
    return output_paths


# Example usage
if __name__ == "__main__":
    # Get the base directory (project root)
    # Script is in: excel_rag_pipeline_pandas/src/pre-proccessing/
    # Base dir is: excel_rag_pipeline_pandas/
    script_dir = Path(__file__).parent.resolve()
    base_dir = script_dir.parent.parent  # Go up two levels to project root
    
    # Define input and output directories
    input_dir = base_dir / "data" / "input"
    output_dir = base_dir / "data" / "preprocessed"
    
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
    
    # Process all Excel files
    output_paths = process_excel_files_batch(
        excel_files=excel_files,
        output_dir=output_dir,
        sheet_name=0,  # First sheet
        header_row=1    # Second row as header (0-indexed)
    )
    
    print(f"\n{'='*60}")
    print(f"✅ Processing complete!")
    print(f"   Processed: {len(output_paths)} file(s)")
    print(f"   Output location: {output_dir}")
    print(f"{'='*60}")
    