
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
import os
import sys
from pathlib import Path
warnings.filterwarnings('ignore')

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from ToxiCRpreTrained import ToxiCR

def process_parquet_with_toxicr(input_file, output_file):
    print(f"Reading Parquet file: {input_file}")
    
    df = pd.read_parquet(input_file)
    print(f"Loaded {len(df)} rows")
    
    if not hasattr(process_parquet_with_toxicr, "toxicr"):
        print(" Loading ToxiCR model ...")
        toxicr = ToxiCR(
            ALGO="BERT",
            embedding="bert", 
            split_identifier=False,
            remove_keywords=True,
            count_profanity=False
        )
        
        if not toxicr.init_predictor():
            print("Failed to load ToxiCR model")
            return False
        
        process_parquet_with_toxicr.toxicr = toxicr
        print("ToxiCR model loaded successfully")
    else:
        print("Reusing existing ToxiCR model")
    
    toxicr = process_parquet_with_toxicr.toxicr
    
    texts = df['text'].fillna("").astype(str).tolist()
    
    print("Starting toxicity prediction with ToxiCR...")
    
    batch_size = 100
    all_scores = []
    
    import tqdm as tqdm_module
    original_tqdm = tqdm_module.tqdm

    class SilentTqdm:
        def __init__(self, *args, **kwargs):
            self.iterable = args[0] if args else None
            
        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])
            
        def __enter__(self):
            return self
            
        def __exit__(self, *args):
            pass
            
        def update(self, *args):
            pass
            
        def close(self):
            pass
    
    tqdm_module.tqdm = SilentTqdm
    try:
        print(f"Processing {len(texts)} texts...")
        progress_bar = original_tqdm(total=len(texts), desc="   Processing", unit="texts")
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i+batch_size]
            
            try:
                # try batch processing
                batch_scores = toxicr.get_toxicity_probability(batch_texts)
                
                if isinstance(batch_scores, (list, np.ndarray)):
                    batch_scores = [float(score) for score in batch_scores]
                else:
                    batch_scores = [float(batch_scores)] * len(batch_texts)
                    
                all_scores.extend(batch_scores)
                
            except Exception as e:
                # batch processing failed, process one by one
                for text in batch_texts:
                    try:
                        score = toxicr.get_toxicity_probability([text])
                        if isinstance(score, (list, np.ndarray)):
                            score = float(score[0])
                        else:
                            score = float(score)
                        all_scores.append(score)
                    except Exception:
                        all_scores.append(0.0)
            
            progress_bar.update(len(batch_texts))
        
        progress_bar.close()
        
    finally:
        # restore original tqdm
        tqdm_module.tqdm = original_tqdm
    
    # Add scores to dataframe
    df['score'] = all_scores
    
    # Save results
    print(f"Saving results to: {output_file}")
    df.to_parquet(output_file, index=False)
    
    print(f"Complete! Stats: Mean={np.mean(all_scores):.4f}, Min={np.min(all_scores):.4f}, Max={np.max(all_scores):.4f}")
    return True

def main():
    base_path = "/home/strrl/ssd"
    folders = ["score_devops", "score_frontend", "score_game", "score_mobile", "score_ml"]
    years = [2019, 2020, 2021, 2022, 2023, 2024]
    print("Starting ToxiCR scoring ...")
    print("=" * 50)

    total_files = len(folders) * len(years)
    processed_files = 0
    failed_files = []

    overall_progress = tqdm(total=total_files, desc="Overall Progress", unit="files")

    for folder in folders:
        print(f"\nProcessing folder: {folder}")
        print("-" * 40)
        
        folder_path = Path(base_path) / folder
        
        # check if folder exists
        if not folder_path.exists():
            print(f"Folder {folder_path} does not exist, skipping...")
            overall_progress.update(len(years))
            failed_files.extend([f"{folder}/{year}.parquet" for year in years])
            continue
        
        # iterate over each year
        for year in years:
            input_file = folder_path / f"{year}.parquet"
            output_file = folder_path / f"{year}_toxicr_score.parquet"
            
            print(f"\nProcessing: {folder}/{year}.parquet")
            
            # check if input file exists
            if not input_file.exists():
                print(f"Input file {input_file} does not exist, skipping...")
                failed_files.append(f"{folder}/{year}.parquet")
            else:
                # check if output file exists
                if output_file.exists():
                    print(f"Output file {output_file} already exists, skipping...")
                    overall_progress.update(1)
                    continue
                
                # process file
                try:
                    success = process_parquet_with_toxicr(str(input_file), str(output_file))
                    if success:
                        processed_files += 1
                        print(f"Successfully processed {folder}/{year}.parquet")
                    else:
                        failed_files.append(f"{folder}/{year}.parquet")
                        print(f"Failed to process {folder}/{year}.parquet")

                except Exception as e:
                    failed_files.append(f"{folder}/{year}.parquet")
                    print(f"Error processing {folder}/{year}.parquet: {str(e)}")
            
            overall_progress.update(1)
    
    overall_progress.close()
    
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Total files to process: {total_files}")
    print(f"Successfully processed: {processed_files}")
    print(f"Failed files: {len(failed_files)}")
    
    if failed_files:
        print("\nFailed files:")
        for file in failed_files:
            print(f"   - {file}")
    
    print(f"\nBatch processing complete!")
    print(f"Success rate: {processed_files/total_files*100:.1f}%")


if __name__ == "__main__":
    main() 