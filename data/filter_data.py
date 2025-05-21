import pandas as pd
import os

def filter_and_merge_csv(input_folder, output_file, exclude_file=None):
    """
    Memfilter baris CSV berdasarkan URL yang mengandung '/hukum-dan-kriminal/'
    dan menggabungkan hasilnya ke dalam satu file CSV.

    Args:
        input_folder (str): Path ke folder yang berisi file-file CSV hasil scraping.
        output_file (str): Nama file CSV output untuk hasil gabungan.
        exclude_file (str, optional): Nama file CSV yang akan dikecualikan.
                                      Contoh: 'detik_kriminalitas_all.csv'.
    """
    all_filtered_data = pd.DataFrame()
    
    for filename in os.listdir(input_folder):
        if exclude_file and filename == exclude_file:
            print(f"Melewati file yang dikecualikan: {filename}")
            continue 

        if filename.endswith(".csv"):
            filepath = os.path.join(input_folder, filename)
            try:
                df = pd.read_csv(filepath)
                
                if 'url' in df.columns:
                    filtered_df = df[df['url'].str.contains('/hukum-dan-kriminal/', na=False)]
                    all_filtered_data = pd.concat([all_filtered_data, filtered_df], ignore_index=True)
                else:
                    print(f"Peringatan: File {filename} tidak memiliki kolom 'url'. Melewati file ini.")
            except Exception as e:
                print(f"Error saat membaca atau memproses file {filename}: {e}")

    # Simpan hasil gabungan ke dalam satu file CSV
    if not all_filtered_data.empty:
        all_filtered_data.to_csv(output_file, index=False)
        print(f"Data yang difilter berhasil digabungkan dan disimpan ke: {output_file}")
    else:
        print("Tidak ada data yang cocok dengan kriteria filter ditemukan.")

if __name__ == "__main__":
    input_directory = '../data/hasil_scraping'
    output_csv_filename = '../data/processed/filtered_all_data.csv'
    
    file_to_exclude = 'detik_kriminalitas_all.csv'
    
    filter_and_merge_csv(input_directory, output_csv_filename, exclude_file=file_to_exclude)