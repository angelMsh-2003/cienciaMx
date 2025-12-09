from fpdf import FPDF
from datetime import datetime
import os

class PDFReport(FPDF):
    def header(self):
        self.set_font('helvetica', 'B', 16)
        self.cell(0, 10, 'OAI-PMH Normalization Report', border=False, align='C')
        self.ln(5)
        self.set_font('helvetica', 'I', 10)
        self.cell(0, 10, f'Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', border=False, align='C')
        self.ln(15)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', align='C')

    def chapter_title(self, title):
        self.set_font('helvetica', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 10, title, border=1, fill=True, align='L')
        self.ln(12)

    def chapter_body(self, text):
        self.set_font('helvetica', '', 10)
        self.multi_cell(0, 5, text)
        self.ln()

    def add_list(self, items):
        self.set_font('helvetica', '', 9)
        for item in items:
            self.cell(10) # Indent
            self.cell(0, 5, f'- {item}', align='L')
            self.ln()
        self.ln()

def generate_pdf_report(stats, output_path="normalization_report.pdf"):
    """
    Generates a PDF report from the provided statistics.
    
    Stats structure expected:
    {
        "total_files": int,
        "processed_files": int,
        "skipped_files": int,
        "total_records_saved": int,
        "empty_repos": [str],
        "duplicate_repos": [str],
        "title_stats": {
            "RepoName": {"single": int, "multi": int, "unknown": int, "total": int}
        }
    }
    """
    pdf = PDFReport()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Calculate Title Grand Totals
    total_single = 0
    total_multi = 0
    total_unknown = 0
    title_stats = stats.get('title_stats', {})
    
    for repo, data in title_stats.items():
        total_single += data.get('single', 0)
        total_multi += data.get('multi', 0)
        total_unknown += data.get('unknown', 0)

    # Summary Section
    pdf.chapter_title("Execution Summary")
    pdf.set_font('helvetica', '', 10)
    pdf.cell(0, 6, f"Total Files Scanned: {stats.get('total_files', 0)}", ln=True)
    pdf.cell(0, 6, f"Files Processed (Saved): {stats.get('processed_files', 0)}", ln=True)
    pdf.cell(0, 6, f"Files Skipped: {stats.get('skipped_files', 0)}", ln=True)
    pdf.cell(0, 6, f"Total Valid Records: {stats.get('total_records_saved', 0)}", ln=True)
    pdf.ln(4)
    pdf.set_font('helvetica', 'B', 10)
    pdf.cell(0, 6, "Global Title Statistics:", ln=True)
    pdf.set_font('helvetica', '', 10)
    pdf.cell(10)
    pdf.cell(0, 6, f"- Records with exactly 1 Title: {total_single}", ln=True)
    pdf.cell(10)
    pdf.cell(0, 6, f"- Records with >1 Titles: {total_multi}", ln=True)
    pdf.cell(10)
    pdf.cell(0, 6, f"- Records with Unknown/0 Titles: {total_unknown}", ln=True)
    pdf.ln(10)
    
    # Empty Repositories
    empty_repos = stats.get('empty_repos', [])
    pdf.chapter_title(f"Empty Repositories ({len(empty_repos)})")
    if empty_repos:
        # Sort empty repos alphabetically
        empty_repos.sort()
        pdf.set_font('helvetica', '', 9)
        pdf.multi_cell(0, 5, "The following repositories contained no valid records or were filtered out completely:")
        pdf.ln(2)
        pdf.add_list(empty_repos)
    else:
        pdf.chapter_body("No empty repositories found.")
        
    # Duplicate Repositories
    duplicate_repos = stats.get('duplicate_repos', [])
    pdf.chapter_title(f"Duplicate Repositories ({len(duplicate_repos)})")
    if duplicate_repos:
        # Sort duplicates alphabetically
        duplicate_repos.sort()
        pdf.set_font('helvetica', '', 9)
        pdf.multi_cell(0, 5, "The following repositories were skipped because ALL their records were already present in previous files:")
        pdf.ln(2)
        pdf.add_list(duplicate_repos)
    else:
        pdf.chapter_body("No fully duplicate repositories found.")
        
    # Title Statistics
    pdf.add_page() # Start stats on new page if needed
    pdf.chapter_title("Title Statistics per Repository")
    
    # Table Header
    pdf.set_font('helvetica', 'B', 8)
    col_w = [85, 30, 30, 30] # Column widths
    pdf.cell(col_w[0], 7, "Repository Name", border=1)
    pdf.cell(col_w[1], 7, "1 Title", border=1, align='C')
    pdf.cell(col_w[2], 7, ">1 Titles", border=1, align='C')
    pdf.cell(col_w[3], 7, "Unknown/0", border=1, align='C')
    pdf.ln()
    
    # Table Data
    pdf.set_font('helvetica', '', 8)
    # Sort alphabetically by repository name
    sorted_stats = sorted(title_stats.items(), key=lambda x: x[0])
    
    for repo, data in sorted_stats:
        # Truncate long names
        display_name = (repo[:55] + '...') if len(repo) > 55 else repo
        
        pdf.cell(col_w[0], 6, display_name, border=1)
        pdf.cell(col_w[1], 6, str(data['single']), border=1, align='C')
        
        # Highlight issues
        if data['multi'] > 0:
            pdf.set_font('helvetica', 'B', 8)
        pdf.cell(col_w[2], 6, str(data['multi']), border=1, align='C')
        pdf.set_font('helvetica', '', 8)
        
        if data['unknown'] > 0:
            pdf.set_font('helvetica', 'B', 8)
        pdf.cell(col_w[3], 6, str(data['unknown']), border=1, align='C')
        pdf.set_font('helvetica', '', 8)
        
        pdf.ln()

    try:
        pdf.output(output_path)
        print(f"[REPORT] PDF Report generated: {output_path}")
    except Exception as e:
        print(f"[ERROR] Failed to generate PDF report: {e}")
