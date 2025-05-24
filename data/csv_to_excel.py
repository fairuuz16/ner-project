import pandas as pd
import ast

csv_file_path = '../data/processed/forlabelling.csv'
excel_file_path = 'pseudo_labelling.xlsx'
excel_sheet_name = 'Data'

def konversi_dan_split_csv_ke_excel(input_csv, output_excel, sheet_name):
    """
    Mengonversi file CSV yang berisi list token dan POS tag dalam string
    menjadi file Excel dengan setiap token pada baris terpisah.
    """
    try:
        # Membaca file CSV
        df_input = pd.read_csv(input_csv)
        
        print(f"Kolom terdeteksi di CSV input: {df_input.columns.tolist()}")
        
        # Memastikan kolom yang dibutuhkan ada
        required_columns = ['id', 'isi_pos_tags', 'isi_stop_removed']
        if not all(col in df_input.columns for col in required_columns):
            print(f"Error: File CSV harus memiliki kolom: {', '.join(required_columns)}.")
            print(f"Kolom yang ditemukan: {df_input.columns.tolist()}")
            return

        output_rows = []

        print("\nMemulai proses transformasi data...")
        for index, row in df_input.iterrows():
            doc_id = row['id']
            pos_tags_str = row['isi_pos_tags']
            stop_removed_str = row['isi_stop_removed']

            try:
                # Menggunakan ast.literal_eval untuk konversi string ke list/tuple Python
                # Ini lebih aman daripada eval()
                parsed_pos_tags = ast.literal_eval(pos_tags_str) # Harusnya menjadi list of tuples, misal: [('kata1', 'TAG1'), ...]
                parsed_stop_removed_tokens = ast.literal_eval(stop_removed_str) # Harusnya menjadi list of strings, misal: ['kata1', ...]
                
                # Validasi tipe data setelah parsing
                if not isinstance(parsed_pos_tags, list):
                    print(f"Peringatan di baris CSV {index + 2} (ID: {doc_id}): 'isi_pos_tags' tidak ter-parsing sebagai list. Isi: {pos_tags_str[:100]}...")
                    continue
                if not isinstance(parsed_stop_removed_tokens, list):
                    print(f"Peringatan di baris CSV {index + 2} (ID: {doc_id}): 'isi_stop_removed' tidak ter-parsing sebagai list. Isi: {stop_removed_str[:100]}...")
                    continue

                # Asumsi utama: panjang kedua list sama dan tokennya berkorespondensi secara urut.
                if len(parsed_pos_tags) != len(parsed_stop_removed_tokens):
                    print(f"Peringatan di baris CSV {index + 2} (ID: {doc_id}): Jumlah item di 'isi_pos_tags' ({len(parsed_pos_tags)}) "
                          f"tidak sama dengan 'isi_stop_removed' ({len(parsed_stop_removed_tokens)}). Baris ini dilewati.")
                    # Untuk debugging, Anda bisa print isinya:
                    # print(f"  isi_pos_tags data: {parsed_pos_tags}")
                    # print(f"  isi_stop_removed data: {parsed_stop_removed_tokens}")
                    continue
                
                # Iterasi berdasarkan token di 'isi_stop_removed'
                for i, token_final in enumerate(parsed_stop_removed_tokens):
                    if i < len(parsed_pos_tags):
                        # Pastikan elemen di parsed_pos_tags adalah tuple dan punya 2 item
                        if not (isinstance(parsed_pos_tags[i], tuple) and len(parsed_pos_tags[i]) == 2):
                            print(f"Peringatan di baris CSV {index + 2} (ID: {doc_id}), item ke-{i+1} di 'isi_pos_tags' bukan tuple (token, tag). Item: {parsed_pos_tags[i]}. Item ini dilewati.")
                            continue

                        token_from_pos_list = parsed_pos_tags[i][0]
                        tag = parsed_pos_tags[i][1]

                        # Peringatan jika token dari kedua sumber tidak cocok (meskipun urutannya sama)
                        if token_from_pos_list != token_final:
                            print(f"Peringatan di baris CSV {index + 2} (ID: {doc_id}), token ke-{i+1}: "
                                  f"'{token_from_pos_list}' (dari isi_pos_tags) berbeda dengan '{token_final}' (dari isi_stop_removed). "
                                  f"Menggunakan token dari 'isi_stop_removed' ('{token_final}') dengan tag '{tag}'.")
                        
                        output_rows.append({
                            'id': doc_id,
                            'token': token_final, # Sesuai permintaan: "pisahkan berdasarkan token di isi stop removed"
                            'postag': tag,
                            'entity': '' # Kolom entity dikosongkan, sesuai contoh gambar
                        })
                    else:
                        # Seharusnya tidak terjadi jika panjang list sudah dicek sama
                        print(f"Error internal: Indeks {i} di luar jangkauan untuk 'parsed_pos_tags' pada ID {doc_id}. Ini tidak seharusnya terjadi.")
                        break 

            except (SyntaxError, ValueError) as e:
                print(f"Error parsing string (SyntaxError/ValueError) di baris CSV {index + 2} (ID: {doc_id}): {e}")
                print(f"  Isi 'isi_pos_tags' (awal): {pos_tags_str[:100]}...")
                print(f"  Isi 'isi_stop_removed' (awal): {stop_removed_str[:100]}...")
                continue # Lanjut ke baris berikutnya di CSV

        if not output_rows:
            print("\nTidak ada data yang berhasil diproses untuk ditulis ke Excel. Periksa pesan error/peringatan di atas.")
            return

        # Membuat DataFrame dari hasil transformasi
        df_output = pd.DataFrame(output_rows)
        
        # Mengatur urutan kolom sesuai contoh (opsional jika urutan sudah benar)
        df_output = df_output[['id', 'token', 'postag', 'entity']]

        # Menulis DataFrame ke file Excel
        df_output.to_excel(output_excel, sheet_name=sheet_name, index=False)
        print(f"\nBerhasil! File CSV '{input_csv}' telah dikonversi dan di-split menjadi '{output_excel}' (sheet: '{sheet_name}').")

    except FileNotFoundError:
        print(f"Error: File CSV '{input_csv}' tidak ditemukan. Pastikan path dan nama file sudah benar.")
    except pd.errors.EmptyDataError:
        print(f"Error: File CSV '{input_csv}' kosong.")
    except Exception as e:
        print(f"Terjadi error umum saat proses konversi: {e}")
        import traceback
        traceback.print_exc()


# Menjalankan fungsi konversi
if __name__ == "__main__":
    # Pastikan nama file CSV (csv_file_path) sudah sesuai dengan file Anda.
    # File tersebut harus ada di direktori yang sama dengan script ini,
    # atau Anda perlu memberikan path lengkap.
    konversi_dan_split_csv_ke_excel(csv_file_path, excel_file_path, excel_sheet_name)
