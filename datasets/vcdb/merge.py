import atexit
import gc
import glob
import os
import sys
from typing import List, Dict, Set, Optional

import polars as pl
import psutil

# Memory limit in bytes (2 GB)
MEMORY_LIMIT = 2 * 1024 ** 3
TEMP_FILES: List[str] = []


def cleanup_temp_files() -> None:
    """Clean up temporary files on exit"""
    for temp_file in TEMP_FILES:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"🗑️  Removed temp file: {temp_file}")
        except Exception as e:
            print(f"❌ Failed to remove {temp_file}: {e}")


def register_temp_file(filepath: str) -> None:
    """Register a temporary file for cleanup"""
    TEMP_FILES.append(filepath)


# Register cleanup function
atexit.register(cleanup_temp_files)


def check_memory_usage() -> int:
    """Check current memory usage and exit if limit exceeded"""
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss

    if memory_usage > MEMORY_LIMIT:
        print(f"❌ Memory limit exceeded: {memory_usage / 1024 ** 3:.2f} GB used.")
        cleanup_temp_files()
        sys.exit("Terminating due to excessive memory usage.")

    print(f"✅ Memory OK: {memory_usage / 1024 ** 3:.2f} GB")
    return memory_usage


def clear_memory() -> None:
    """Force garbage collection"""
    gc.collect()


def analyze_id_overlap(files: List[str], join_key: str) -> Dict[str, Dict]:
    """Analyze ID overlap between files"""
    print("🔍 ID OVERLAP ANALYSIS")
    print("=" * 50)

    ids_by_file = {}

    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"📄 Analyzing {filename}...")

        df = pl.read_csv(file_path)
        unique_ids = set(df[join_key].unique().to_list())

        ids_by_file[filename] = {
            'ids': unique_ids,
            'unique_count': len(unique_ids),
            'total_rows': len(df)
        }

        duplicates_count = len(df) - len(unique_ids)
        print(f"   📊 Unique IDs: {len(unique_ids)}")
        print(f"   📊 Total rows: {len(df)}")
        print(f"   📊 Duplicates in file: {duplicates_count}")

        del df
        clear_memory()

    # Analyze overlap between files
    print(f"\n🔍 OVERLAP ANALYSIS:")
    filenames = list(ids_by_file.keys())

    for i, file1 in enumerate(filenames):
        for file2 in filenames[i + 1:]:
            ids1 = ids_by_file[file1]['ids']
            ids2 = ids_by_file[file2]['ids']

            intersection = ids1.intersection(ids2)
            union = ids1.union(ids2)

            overlap_percent = len(intersection) / len(union) * 100 if union else 0

            print(f"   🔗 {file1} ∩ {file2}:")
            print(f"      🎯 Common IDs: {len(intersection)}")
            print(f"      📊 Total unique: {len(union)}")
            print(f"      📈 Overlap: {overlap_percent:.1f}%")

    return ids_by_file


def get_common_ids(files: List[str], join_key: str) -> Set:
    """Get IDs that exist in ALL files"""
    common_ids = None

    for file_path in files:
        df = pl.read_csv(file_path)
        file_ids = set(df[join_key].unique().to_list())

        if common_ids is None:
            common_ids = file_ids
        else:
            common_ids = common_ids.intersection(file_ids)

        del df
        clear_memory()

    return common_ids or set()


def get_all_unique_ids(files: List[str], join_key: str) -> Set:
    """Get all unique IDs from all files"""
    all_ids = set()

    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"   📄 Reading IDs from {filename}...")

        df_ids = pl.read_csv(file_path).select(join_key)
        unique_ids = df_ids[join_key].unique().to_list()

        print(f"      📊 {len(unique_ids)} unique IDs")
        all_ids.update(unique_ids)

        del df_ids
        clear_memory()
        check_memory_usage()

    return all_ids


def controlled_join_with_base(files: List[str], join_key: str, use_intersection: bool = True) -> Optional[pl.DataFrame]:
    """Perform controlled join using ID base"""
    print("🎯 CONTROLLED JOIN WITH ID BASE")

    # Analyze overlap first
    print("\n🔍 Analyzing overlap...")
    ids_info = analyze_id_overlap(files, join_key)

    if use_intersection:
        print("\n🎯 Strategy: INTERSECTION (IDs existing in ALL files)")
        common_ids = get_common_ids(files, join_key)

        print(f"   🎯 Common IDs across all files: {len(common_ids)}")

        if len(common_ids) == 0:
            print("❌ No common IDs between all files!")
            return None

        base_df = pl.DataFrame({join_key: list(common_ids)})
    else:
        print("\n🎯 Strategy: UNION (all unique IDs)")
        all_ids = get_all_unique_ids(files, join_key)
        print(f"\n🎯 Total unique IDs across all files: {len(all_ids)}")
        base_df = pl.DataFrame({join_key: list(all_ids)})

    print(f"   📊 Base created: {base_df.shape}")
    check_memory_usage()

    result_df = base_df

    for i, file_path in enumerate(files):
        filename = os.path.basename(file_path)
        print(f"\n🔗 Join {i + 1}/{len(files)}: {filename}")

        temp_df = pl.read_csv(file_path)
        print(f"   📄 Original file: {temp_df.shape}")

        # Remove duplicates by ID (keep first occurrence)
        temp_df_unique = temp_df.unique(subset=[join_key], keep="first")
        print(f"   📄 After removing duplicates: {temp_df_unique.shape}")

        # Identify new columns
        new_columns = [col for col in temp_df_unique.columns if col not in result_df.columns]

        if new_columns:
            print(f"   📋 New columns: {new_columns}")

            # Select only new columns + join key
            required_columns = [join_key] + new_columns
            temp_df_select = temp_df_unique.select(required_columns)

            # LEFT JOIN (maintain all IDs from base)
            result_df = result_df.join(temp_df_select, on=join_key, how="left")
            print(f"   📊 Result: {result_df.shape}")

            # Critical check: if row count changed, something is wrong
            if result_df.height != base_df.height:
                print(f"   ❌ ERROR! Row count changed: {base_df.height} → {result_df.height}")
                print("   🛑 Stopping to prevent memory explosion!")
                break

            del temp_df_select
        else:
            print(f"   ⚠️  No new columns in {filename}")

        del temp_df, temp_df_unique
        clear_memory()
        check_memory_usage()

    return result_df


def main_file_strategy(files: List[str], join_key: str, main_file: Optional[str] = None) -> Optional[pl.DataFrame]:
    """Use a specific file as the main base"""
    print("🎯 STRATEGY: MAIN FILE AS BASE")

    # Choose main file
    if main_file:
        base_file = None
        for file_path in files:
            if main_file in os.path.basename(file_path):
                base_file = file_path
                break

        if not base_file:
            print(f"❌ File '{main_file}' not found!")
            return None
    else:
        base_file = files[0]

    print(f"🟢 Base file: {os.path.basename(base_file)}")

    # Load base file
    result_df = pl.read_csv(base_file)
    print(f"   📊 Base: {result_df.shape}")

    # Remove duplicates from base
    result_df = result_df.unique(subset=[join_key], keep="first")
    print(f"   📊 Base without duplicates: {result_df.shape}")
    check_memory_usage()

    # Join with other files
    other_files = [f for f in files if f != base_file]

    for i, file_path in enumerate(other_files):
        filename = os.path.basename(file_path)
        print(f"\n🔗 Join {i + 1}/{len(other_files)}: {filename}")

        temp_df = pl.read_csv(file_path)
        print(f"   📄 File: {temp_df.shape}")

        # Remove duplicates
        temp_df = temp_df.unique(subset=[join_key], keep="first")
        print(f"   📄 Without duplicates: {temp_df.shape}")

        # New columns
        new_columns = [col for col in temp_df.columns if col not in result_df.columns]

        if new_columns:
            temp_df = temp_df.select([join_key] + new_columns)
            print(f"   📋 Selected: {len(new_columns)} columns")

            # LEFT JOIN
            rows_before = result_df.height
            result_df = result_df.join(temp_df, on=join_key, how="left")

            print(f"   📊 Result: {result_df.shape}")

            # Critical check
            if result_df.height != rows_before:
                print(f"   ❌ ERROR! Row count changed: {rows_before} → {result_df.height}")
                print("   🛑 Interrupting!")
                break

        del temp_df
        clear_memory()
        check_memory_usage()

    return result_df


def save_result(df: pl.DataFrame, output_path: str = "./_tmp/result.csv") -> None:
    """Save the result DataFrame"""
    df.write_csv(output_path)
    print(f"💾 Saved: {output_path}")


def print_statistics(df: pl.DataFrame, join_key: str) -> None:
    """Print DataFrame statistics"""
    print(f"\n📈 Statistics:")
    print(f"   🎯 Total rows: {df.height:,}")
    print(f"   📋 Total columns: {df.width}")
    print(f"   🔍 Unique IDs: {df[join_key].n_unique()}")

    print(f"\n👀 Preview:")
    print(df.head())


def run_merge() -> None:
    """Main execution function"""
    data_path = "./_tmp"
    join_key = "incident_id"
    csv_files = glob.glob(os.path.join(data_path, "*.csv"))

    if len(csv_files) < 2:
        raise Exception("At least two CSV files are required.")

    print(f"📁 Found {len(csv_files)} files")
    print("🎯 Identified problem: CARTESIAN PRODUCT in joins!")
    print("💡 Available solutions:\n")

    try:
        # Choose strategy automatically
        print("🎯 Choosing strategy automatically...")

        if len(csv_files) <= 5:
            print("📊 Using: INTERSECTION (common IDs across all files)")
            final_df = controlled_join_with_base(csv_files, join_key, use_intersection=True)
        else:
            print("📊 Using: MAIN FILE as base")
            final_df = main_file_strategy(csv_files, join_key)

        if final_df is not None:
            print(f"\n✅ Controlled join completed!")
            print(f"📊 Final result: {final_df.shape}")

            # Save result
            save_result(final_df)

            # Print statistics
            print_statistics(final_df, join_key)

            del final_df
            clear_memory()
            check_memory_usage()

    except Exception as e:
        print(f"❌ Error: {e}")
        check_memory_usage()

    finally:
        # Cleanup temporary files
        cleanup_temp_files()
