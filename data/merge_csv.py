import pandas as pd
import os
import glob

def combine_csv_files():
    """Menggabungkan semua file batch CSV menjadi satu file"""
    try:
        print("Menggabungkan file batch CSV...")
        
        if not os.path.exists('hasil_scraping'):
            print("Folder hasil_scraping tidak ditemukan!")
            return
        
        batch_pattern = os.path.join('hasil_scraping', 'detik_kriminalitas_*_to_*.csv')
        all_files = sorted(glob.glob(batch_pattern))
        
        if not all_files:
            print("Tidak ada file batch yang ditemukan untuk digabungkan.")
            return
            
        print(f"Menemukan {len(all_files)} file untuk digabungkan:")
        for file in all_files:
            print(f"- {os.path.basename(file)}")
            
        all_dfs = []
        
        for file in all_files:
            try:
                df = pd.read_csv(file)
                if df.empty:
                    print(f"File {os.path.basename(file)} kosong, akan dilewati.")
                    continue
                    
                print(f"Membaca {os.path.basename(file)}: {len(df)} baris")
                all_dfs.append(df)
            except Exception as e:
                print(f"Error saat membaca file {os.path.basename(file)}: {e}")
                continue
        
        if not all_dfs:
            print("Tidak ada data valid yang bisa digabungkan.")
            return
            
        # Gabungkan semua dataframe
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Hapus duplikat jika ada
        original_count = len(combined_df)
        combined_df.drop_duplicates(subset=['url'], keep='first', inplace=True)
        dedup_count = len(combined_df)
        
        if original_count > dedup_count:
            print(f"Menghapus {original_count - dedup_count} duplikat data.")
            
        # Simpan hasil gabungan
        output_file = os.path.join('hasil_scraping', 'detik_kriminalitas_all.csv')
        combined_df.to_csv(output_file, index=False)
        print(f"Semua data berhasil digabungkan. Total {len(combined_df)} artikel tersimpan ke {output_file}")
            
    except Exception as e:
        print(f"Error saat menggabungkan file: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    combine_csv_files()