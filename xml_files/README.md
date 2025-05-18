# XML Files Directory

This directory is where you should place IRS 990 XML files to be processed by Blizzard 2.1.

## Usage

1. Place your IRS 990 XML files directly in this directory, or in subdirectories organized by year, form type, or any other organizational scheme you prefer.

2. Run the ETL process by executing one of the following commands from the Blizzard 2.1 root directory:

   ```bash
   # Process all XML files in this directory
   python -m scripts.run_etl

   # Process files with additional options
   python -m scripts.run_etl --batch-size 50 --workers 4
   
   # Process files in batches with reporting
   python -m scripts.batch_etl --limit 100 --report
   ```

3. Results will be stored in the database and processing logs will be available in the `logs` directory.

## Recommendations

- For large volumes of XML files, organize them in subdirectories by year (e.g., `2023/`, `2024/`, etc.)
- If files come from different sources or form types, you might want to organize them accordingly (e.g., `990/`, `990EZ/`, `990PF/`)
- For production environments, consider setting up a file watcher or scheduled job to automatically process new files

## Examples

Example directory structure:

```
xml_files/
├── 2023/
│   ├── 990_123456789_202301.xml
│   ├── 990_234567890_202302.xml
│   └── ...
├── 2024/
│   ├── 990_345678901_202401.xml
│   └── ...
└── incoming/
    └── new_files_to_process.xml
```

You can process specific subdirectories:

```bash
python -m scripts.run_etl --xml-dir xml_files/2024
```

Or process a limited number of files:

```bash
python -m scripts.batch_etl --limit 50
```
