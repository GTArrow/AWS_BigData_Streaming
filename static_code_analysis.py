from bandit.core import manager as bandit_manager
from bandit.core import config as bandit_config
import subprocess
import os
from fpdf import FPDF

# Directory containing the Python files
target_directory = "./"

# Output Bandit result to a temporary file
output_file = "bandit_results.txt"

# Run Bandit on the directory
subprocess.run(["bandit", "-r", target_directory, "-f", "txt", "-o", output_file])

# Create a PDF
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Bandit Static Code Analysis Report', 0, 1, 'C')
        self.ln(10)

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(5)

    def chapter_body(self, body):
        self.set_font('Arial', '', 10)
        self.multi_cell(0, 10, body)
        self.ln()

    def add_bandit_report(self, file_path):
        with open(file_path, 'r') as file:
            content = file.read()
            self.chapter_body(content)

# Generate PDF
pdf = PDF()
pdf.add_page()
pdf.chapter_title("Bandit Report")
pdf.add_bandit_report(output_file)

# Save PDF to disk
pdf_file_path = "Bandit_Report.pdf"
pdf.output(pdf_file_path)

# Clean up temporary file
os.remove(output_file)

pdf_file_path
