import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from datetime import datetime
import os

class DetikKriminalitasScraper:
    def __init__(self):
        # User agent untuk menghindari blocking
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Dataframe untuk menyimpan hasil
        self.df = pd.DataFrame(columns=['url', 'judul', 'subjudul', 'tanggal', 'isi_berita', 'kanal'])
        
        # Buat folder untuk menyimpan hasil jika belum ada
        if not os.path.exists('hasil_scraping'):
            os.makedirs('hasil_scraping')
        
        # File untuk menyimpan progress
        self.progress_file = 'hasil_scraping/progress.txt'
        
    def save_progress(self, page):
        """Menyimpan progress scraping"""
        with open(self.progress_file, 'w') as f:
            f.write(str(page))
            
    def load_progress(self):
        """Memuat progress scraping sebelumnya jika ada"""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                try:
                    return int(f.read().strip())
                except:
                    return 1
        return 1
    
    def save_batch(self, start_page, end_page):
        """Menyimpan batch data ke CSV"""
        filename = f'hasil_scraping/detik_kriminalitas_{start_page}_to_{end_page}.csv'
        self.df.to_csv(filename, index=False)
        print(f"Data halaman {start_page}-{end_page} tersimpan ke {filename}")
        
        # Reset dataframe setelah disimpan untuk menghemat memori
        self.df = pd.DataFrame(columns=['url', 'judul', 'subjudul', 'tanggal', 'isi_berita', 'kanal'])
        
    def save_data(self, filename='hasil_scraping/detik_kriminalitas_all.csv'):
        """Menyimpan data ke CSV"""
        self.df.to_csv(filename, index=False)
        print(f"Data tersimpan ke {filename}")
        
    def random_delay(self, min_sec=1, max_sec=3):
        """Delay acak untuk menghindari deteksi bot"""
        time.sleep(random.uniform(min_sec, max_sec))
    
    def clean_text(self, text):
        """Membersihkan text dari karakter yang tidak perlu"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)  # Mengganti multiple spaces dengan single space
        text = text.strip()
        return text
    
    def format_date(self, date_str):
        """Mengubah string tanggal ke format standar"""
        try:
            # Format tanggal detik: 'Kamis, 16 Mei 2025 13:56 WIB'
            date_match = re.search(r'(\d+) (\w+) (\d+)', date_str)
            if date_match:
                day, month, year = date_match.groups()
                
                # Konversi bulan bahasa Indonesia ke angka
                month_dict = {
                    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 
                    'Mei': '05', 'Jun': '06', 'Jul': '07', 'Agu': '08',
                    'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12',
                    # Alternatif format bulan
                    'Januari': '01', 'Februari': '02', 'Maret': '03', 'April': '04',
                    'Mei': '05', 'Juni': '06', 'Juli': '07', 'Agustus': '08',
                    'September': '09', 'Oktober': '10', 'November': '11', 'Desember': '12'
                }
                
                month_num = month_dict.get(month, '01')  # Default ke 01 jika tidak ditemukan
                
                # Ambil waktu jika ada
                time_match = re.search(r'(\d+):(\d+)', date_str)
                if time_match:
                    hour, minute = time_match.groups()
                    return f"{year}-{month_num}-{day.zfill(2)} {hour}:{minute}"
                else:
                    return f"{year}-{month_num}-{day.zfill(2)}"
            return date_str
        except:
            return date_str
    
    def scrape_search_results(self, query="kriminalitas", result_type="latest", start_page=None, end_page=None, batch_size=10):
        """Scraping hasil pencarian berita dari Detik.com"""
        
        # Jika start_page tidak ditentukan, coba load dari progress
        if start_page is None:
            start_page = self.load_progress()
            print(f"Melanjutkan scraping dari halaman {start_page}")
        
        # Jika end_page tidak ditentukan, set ke angka besar
        if end_page is None:
            end_page = 579  # Sesuai dengan informasi total halaman dari screenshot
            
        current_batch_start = start_page
        
        for page in range(start_page, end_page + 1):
            try:
                print(f"Scraping halaman {page} dari {end_page}...")
                
                # URL pencarian Detik dengan query dan tipe hasil
                url = f"https://www.detik.com/search/searchnews?query={query}&result_type={result_type}&page={page}"
                
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200:
                    print(f"Gagal mengakses halaman {page}: Status code {response.status_code}")
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Cari semua artikel dalam hasil pencarian
                articles = soup.select('article')
                
                if not articles:
                    print(f"Tidak ada artikel ditemukan di halaman {page}. Mencoba selector lain...")
                    articles = soup.select('div.list-berita')
                    
                if not articles:
                    articles = soup.select('a[href*="/berita/"]')
                
                print(f"Menemukan {len(articles)} artikel di halaman {page}")
                
                for article in articles:
                    try:
                        # Coba ekstrak link berita dari artikel
                        link_element = article.select_one('a')
                        if not link_element:
                            continue
                            
                        article_url = link_element.get('href')
                        if not article_url:
                            continue
                            
                        # Pastikan URL lengkap
                        if not article_url.startswith('http'):
                            article_url = f"https:{article_url}"
                            
                        print(f"Memproses artikel: {article_url}")
                        
                        # Delay sebelum request ke halaman artikel
                        self.random_delay(2, 4)
                        
                        # Mengakses halaman artikel
                        try:
                            article_response = requests.get(article_url, headers=self.headers, timeout=10)
                            if article_response.status_code != 200:
                                print(f"Gagal mengakses artikel: {article_url}")
                                continue
                                
                            article_soup = BeautifulSoup(article_response.text, 'html.parser')
                            
                            # Ekstrak judul
                            title = article_soup.select_one('h1.detail__title')
                            if not title:
                                title = article_soup.select_one('h1')
                                
                            title_text = self.clean_text(title.text) if title else "Judul tidak ditemukan"
                            
                            # Ekstrak subjudul
                            subtitle = article_soup.select_one('div.detail__subtitle')
                            subtitle_text = self.clean_text(subtitle.text) if subtitle else ""
                            
                            # Ekstrak tanggal
                            date = article_soup.select_one('div.detail__date')
                            if not date:
                                date = article_soup.select_one('div.date')
                                
                            date_text = self.clean_text(date.text) if date else ""
                            formatted_date = self.format_date(date_text)
                            
                            # Ekstrak kanal/kategori
                            kanal = ""
                            kanal_element = article_soup.select_one('div.breadcrumb')
                            if kanal_element:
                                kanal_links = kanal_element.select('a')
                                if len(kanal_links) > 0:
                                    kanal = self.clean_text(kanal_links[0].text)
                            
                            # Ekstrak isi berita
                            content_elements = article_soup.select('div.detail__body-text p')
                            if not content_elements:
                                content_elements = article_soup.select('div.itp_bodycontent p')
                                
                            content = ' '.join([self.clean_text(p.text) for p in content_elements])
                            
                            # Filter artikel yang tidak memiliki konten
                            if not content:
                                print(f"Artikel tidak memiliki konten yang cukup: {article_url}")
                                continue
                                
                            # Tambahkan ke dataframe
                            new_row = {
                                'url': article_url,
                                'judul': title_text,
                                'subjudul': subtitle_text,
                                'tanggal': formatted_date,
                                'isi_berita': content,
                                'kanal': kanal
                            }
                            self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
                            print(f"Berhasil mengekstrak: {title_text}")
                            
                        except requests.exceptions.Timeout:
                            print(f"Timeout saat mengakses artikel: {article_url}")
                            continue
                        except requests.exceptions.RequestException as e:
                            print(f"Error saat mengakses artikel {article_url}: {e}")
                            continue
                    
                    except Exception as e:
                        print(f"Error pada pemrosesan artikel: {e}")
                        continue
                
                # Simpan progress
                self.save_progress(page)
                
                # Simpan data secara batch untuk menghemat memori
                if page % batch_size == 0 or page == end_page:
                    self.save_batch(current_batch_start, page)
                    current_batch_start = page + 1
                
                # Delay sebelum ke halaman berikutnya
                self.random_delay(3, 6)
                
            except KeyboardInterrupt:
                print("\nProses dihentikan oleh pengguna.")
                print("Menyimpan data yang sudah diambil...")
                self.save_batch(current_batch_start, page-1)
                self.save_progress(page)
                return
                
            except Exception as e:
                print(f"Error pada halaman {page}: {e}")
                self.save_batch(current_batch_start, page-1)
                self.save_progress(page)
                continue
        
        print("\nSelesai scraping!")
        
        # Gabungkan semua file batch jika diperlukan
        self.combine_batch_files()
        
    def combine_batch_files(self):
        """Menggabungkan semua file batch menjadi satu file CSV"""
        try:
            print("Menggabungkan file batch...")
            
            all_files = [f for f in os.listdir('hasil_scraping') if f.startswith('detik_kriminalitas_') and f.endswith('.csv')]
            all_dfs = []
            
            for file in all_files:
                file_path = os.path.join('hasil_scraping', file)
                df = pd.read_csv(file_path)
                all_dfs.append(df)
                
            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                combined_df.to_csv('hasil_scraping/detik_kriminalitas_all.csv', index=False)
                print(f"Semua data digabungkan. Total {len(combined_df)} artikel tersimpan.")
            else:
                print("Tidak ada file batch yang ditemukan untuk digabungkan.")
                
        except Exception as e:
            print(f"Error saat menggabungkan file: {e}")

# Jalankan scraper
if __name__ == "__main__":
    # Parameter yang bisa diubah
    START_PAGE = 51  # Mulai dari halaman berapa
    END_PAGE = 70  # Sampai halaman berapa (total 579 halaman)
    BATCH_SIZE = 10  # Simpan data setiap berapa halaman
    
    scraper = DetikKriminalitasScraper()
    
    try:
        scraper.scrape_search_results(
            query="kriminalitas", 
            result_type="latest",
            start_page=START_PAGE,
            end_page=END_PAGE,
            batch_size=BATCH_SIZE
        )
    except KeyboardInterrupt:
        print("\nProgram dihentikan. Pastikan data terakhir telah disimpan.")