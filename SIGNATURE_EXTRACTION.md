# Signature Image BLOB Extraction

This feature allows you to extract signature images stored as BLOB objects in your Oracle database and save them as image files.

## Overview

The signature extraction feature (`-mode S`) queries your Oracle database to retrieve signature images stored as BLOBs and saves them as individual image files with automatic format detection and proper file naming.

## Features

- **BLOB to Image Conversion**: Extracts Oracle BLOB data and saves as image files
- **Format Auto-Detection**: Automatically detects image format (JPG, PNG, GIF, BMP, TIFF)
- **Flexible Naming**: Use database columns or generate names with custom prefixes
- **Directory Organization**: Option to create subdirectories per SOL ID
- **Batch Processing**: Processes multiple SOLs efficiently with configurable batch sizes
- **Error Handling**: Comprehensive error reporting with detailed logging
- **Progress Tracking**: Real-time progress bars and status updates

## Configuration

Add a `signature_extraction` section to your extraction configuration JSON file:

```json
{
  "package_name": "SIGNATURE_PKG",
  "procedures": [],
  "spool_output_path": "/tmp/signature_output",
  "run_insertion_parallel": false,
  "run_extraction_parallel": false,
  "template_path": "/tmp/templates",
  "format": "delimited",
  "delimiter": ",",
  "signature_extraction": {
    "signature_query": "SELECT signature_id, signature_blob, document_name FROM signatures WHERE sol_id = :1",
    "signature_output_directory": "/tmp/signatures",
    "filename_column": "document_name",
    "filename_prefix": "signature",
    "image_format": "",
    "create_subdirectories": true
  }
}
```

### Configuration Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `signature_query` | Yes | SQL query to extract signature BLOBs. Must include `:1` parameter for SOL ID |
| `signature_output_directory` | Yes | Directory where signature image files will be saved |
| `filename_column` | No | Database column containing the desired filename (without extension) |
| `filename_prefix` | No | Prefix to add to generated filenames |
| `image_format` | No | Force specific image format extension (jpg, png, gif, bmp, tiff). Leave empty for auto-detection |
| `create_subdirectories` | No | Create subdirectories for each SOL ID (default: false) |

### Query Requirements

Your SQL query must:
1. Include a parameter placeholder (`:1` or `?`) for the SOL ID
2. Return at least one BLOB column containing the signature image data
3. Optionally return a column for the filename if using `filename_column`

**Example queries:**

```sql
-- Simple signature extraction
SELECT signature_blob FROM user_signatures WHERE sol_id = :1

-- With custom filename from database
SELECT signature_id, signature_data, document_name 
FROM signatures 
WHERE sol_id = :1 AND signature_type = 'PRIMARY'

-- Multiple signatures per SOL
SELECT signature_blob, signature_type || '_' || sequence_no as filename
FROM digital_signatures 
WHERE sol_id = :1
ORDER BY sequence_no
```

## Usage

Run the application with signature extraction mode:

```bash
# Basic signature extraction
./claude_extract -appCfg app.json -runCfg signature_config.json -mode S

# With verbose logging
./claude_extract -appCfg app.json -runCfg signature_config.json -mode S -verbose

# Quiet mode (minimal output)
./claude_extract -appCfg app.json -runCfg signature_config.json -mode S -quiet
```

## File Naming Logic

The system determines filenames using this priority:

1. **Database Column**: If `filename_column` is specified and contains data
2. **SOL ID Fallback**: Uses the SOL ID if no filename column or empty
3. **Prefix Addition**: Adds `filename_prefix` if configured
4. **Extension Detection**: Automatically detects and adds correct extension

**Examples:**
- With `filename_column="doc_name"` and `filename_prefix="sig"`: `sig_john_doe_signature.jpg`
- With SOL ID "12345" and no prefix: `12345.png` 
- With prefix "signature" and SOL ID: `signature_12345.gif`

## Directory Structure

### Without Subdirectories (`create_subdirectories: false`)
```
/tmp/signatures/
├── signature_12345.jpg
├── signature_12346.png
└── signature_12347.gif
```

### With Subdirectories (`create_subdirectories: true`)
```
/tmp/signatures/
├── 12345/
│   └── signature_12345.jpg
├── 12346/
│   └── signature_12346.png
└── 12347/
    └── signature_12347.gif
```

## Supported Image Formats

The system automatically detects these image formats from BLOB headers:

| Format | Extensions | Magic Bytes |
|--------|------------|-------------|
| JPEG | `.jpg` | `FF D8 FF` |
| PNG | `.png` | `89 50 4E 47` |
| GIF | `.gif` | `47 49 46 38` |
| BMP | `.bmp` | `42 4D` |
| TIFF | `.tiff` | `49 49 2A 00` or `4D 4D 00 2A` |
| Unknown | `.bin` | Binary fallback |

## Error Handling

The system provides detailed error reporting for:

- **Database Connection Issues**: Connection failures, query errors
- **BLOB Data Problems**: Empty BLOBs, invalid data types, corruption
- **File System Issues**: Permission errors, disk space, invalid paths
- **Configuration Errors**: Invalid queries, missing directories

Common error codes:
- `SIGNATURE_DIR_ERROR`: Cannot create output directory
- `SIGNATURE_QUERY_ERROR`: SQL query execution failed
- `SIGNATURE_NOT_FOUND`: No signature data for SOL ID
- `SIGNATURE_BLOB_COLUMN_ERROR`: No BLOB column found in results
- `SIGNATURE_WRITE_ERROR`: Failed to write image file

## Performance Considerations

- **Batch Processing**: Processes signatures in batches to avoid memory issues
- **Connection Pooling**: Reuses database connections efficiently
- **Memory Management**: Streams BLOB data to avoid loading all images into memory
- **Concurrent Processing**: Configurable batch sizes for optimal throughput

## Logging and Monitoring

### Structured Logging
All operations are logged with structured data:
```json
{
  "timestamp": "2024-07-22 17:30:15",
  "level": "info",
  "message": "signature extracted successfully",
  "sol_id": "12345",
  "filename": "signature_12345.jpg",
  "file_size": 15420,
  "format": "jpg",
  "duration": "45ms"
}
```

### Progress Tracking
Real-time progress with:
- Current/total signatures processed
- Success/failure counts
- Processing rate and ETA
- File size statistics

### Summary Reports
Final execution summary includes:
- Total signatures processed
- Success/failure breakdown
- Total file sizes
- Processing duration
- Average processing time per signature

## Troubleshooting

### Common Issues

**No signatures found for SOL ID**
- Verify your query returns data for the test SOL IDs
- Check the parameter placeholder (`:1` vs `?`)
- Ensure SOL IDs exist in your signature table

**BLOB column not detected**
- The system looks for columns containing 'signature', 'image', or 'blob'
- Rename your BLOB column or ensure it contains these keywords
- Check that your query actually returns the BLOB column

**Permission denied writing files**
- Ensure the output directory is writable
- Check disk space availability
- Verify directory path exists and is accessible

**Invalid image format**
- Some BLOBs may not be images - check your database data
- Use `image_format` to force a specific extension if needed
- Check the BLOB data for corruption

### Debug Mode
Use `-verbose` flag for detailed logging:
```bash
./claude_extract -appCfg app.json -runCfg signature_config.json -mode S -verbose
```

This will show:
- SQL query execution details
- BLOB data size and format detection
- File write operations
- Performance metrics
- Detailed error stack traces

## Examples

### Extract User Signatures
```json
{
  "signature_extraction": {
    "signature_query": "SELECT user_signature FROM user_profiles WHERE sol_id = :1 AND signature_blob IS NOT NULL",
    "signature_output_directory": "/data/user_signatures",
    "filename_prefix": "user",
    "create_subdirectories": true
  }
}
```

### Extract Document Signatures with Names
```json
{
  "signature_extraction": {
    "signature_query": "SELECT signature_data, signer_name FROM document_signatures WHERE document_id = :1",
    "signature_output_directory": "/data/document_signatures", 
    "filename_column": "signer_name",
    "filename_prefix": "doc_signature",
    "image_format": "png",
    "create_subdirectories": false
  }
}
```

### Multiple Signatures per Document
```json
{
  "signature_extraction": {
    "signature_query": "SELECT signature_blob, 'page_' || page_number || '_sig_' || signature_sequence as filename FROM page_signatures WHERE document_id = :1 ORDER BY page_number, signature_sequence",
    "signature_output_directory": "/data/page_signatures",
    "filename_column": "filename", 
    "create_subdirectories": true
  }
}
```