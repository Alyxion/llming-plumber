# Test Fixture Sources

## Downloaded from MIT-licensed repos

| File | Source | License |
|---|---|---|
| `sample.pdf` | [jsvine/pdfplumber](https://github.com/jsvine/pdfplumber) `tests/pdfs/pdffill-demo.pdf` | MIT |
| `sample_text.pdf` | [jsvine/pdfplumber](https://github.com/jsvine/pdfplumber) `tests/pdfs/scotus-transcript-p1.pdf` | MIT |
| `sample.docx` | [python-openxml/python-docx](https://github.com/python-openxml/python-docx) `tests/test_files/test.docx` | MIT |
| `sample_images.docx` | [python-openxml/python-docx](https://github.com/python-openxml/python-docx) `tests/test_files/having-images.docx` | MIT |
| `sample.pptx` | [scanny/python-pptx](https://github.com/scanny/python-pptx) `tests/test_files/test.pptx` | MIT |
| `sample.xlsx` | [fluidware/openpyxl](https://github.com/fluidware/openpyxl) `tests/test_data/genuine/guess_types.xlsx` | MIT |
| `sample_empty.xlsx` | [fluidware/openpyxl](https://github.com/fluidware/openpyxl) `tests/test_data/genuine/empty.xlsx` | MIT |

## Generated (our own code, AGPL-3.0)

| File | Generator | Content |
|---|---|---|
| `known_data.xlsx` | `generate_fixtures.py` | 2 sheets (Sales + Metadata), 3+2 data rows |
| `known_data.docx` | `generate_fixtures.py` | Headings, paragraphs, 1 table (2×3) |
| `known_data.pptx` | `generate_fixtures.py` | 3 slides (title, content, blank) |
| `known_data.parquet` | `generate_fixtures.py` | 5 rows × 4 columns (id, name, value, active) |
| `known_data.yaml` | `generate_fixtures.py` | Nested config structure |
| `known_multi.yaml` | `generate_fixtures.py` | Multi-document YAML (2 docs) |
| `known_data.md` | `generate_fixtures.py` | Headings, table, code block |
| `known_data.json` | `generate_fixtures.py` | Pipeline list with metadata |
| `known_data.csv` | `generate_fixtures.py` | 3 data rows × 4 columns |
