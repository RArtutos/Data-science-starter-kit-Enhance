# DuckDB Analysis Environment for Book Metadata

> **Note:** The default configuration is optimized for systems with 30GB RAM. If you have more resources available, you can adjust the parameters accordingly, especially the chunk size (currently set to 10MB) for file processing.

## 🌟 Features

- **Data Processing Pipeline**
  - GZ file decompression
  - JSON to Parquet conversion
  - Chunked processing for large datasets (10MB chunks by default)
  - Memory-efficient operations

- **Analysis Capabilities**
  - Advanced SQL queries with DuckDB
  - Interactive data exploration
  - Rich visualizations with Matplotlib and Seaborn
  - Statistical analysis tools

- **Technical Stack**
  - DuckDB for high-performance SQL queries
  - Jupyter for interactive analysis
  - Pandas for data manipulation
  - Matplotlib & Seaborn for visualization
  - Docker for environment consistency
  
- **Smart Schema Analysis**
   - Automated JSON structure analysis:
   - Samples 10% of dataset for efficient schema detection (configurable)
   - Intelligent column creation based on key frequency (>50% threshold, adjustable)
   - Optimized memory usage through chunked processing
   - Dynamic schema evolution handling

## 📥 Data Setup

### System Requirements
- Minimum: 30GB RAM (default configuration)
- Storage: Depends on dataset size
- Adjustable parameters:
  - Chunk size: Default 10MB (can be increased with more RAM)
  - DuckDB memory limit: Default 28GB (can be adjusted in configuration)

### File Structure
```
project/
├── data/
│   ├── elasticsearch/    # Original .gz files from Elasticsearch
│   ├── elasticsearchF/   #Final .parquet files from Elasticsearch
│   ├── elasticsearchAux/ # Additional Elasticsearch data
│   ├── elasticsearchAuxF/#Final .parquet files from Elasticsearchaux
│   ├── aac/             # AAC Data .zst files
│   ├── aacF/            # Final .parquet files from aac data
├── notebooks/
│   ├── Elasticsearch_Queries.ipynb   # Elasticsearch analysis notebook
│   ├── ElasticsearchAux_Queries.ipynb # ElasticsearchAux analysis notebook
│   └── AAC_Queries.ipynb              # AACanalysis notebooks
```

### Data Processing Steps

1. **Elasticsearch Data**
   - Place `.gz` files in `data/elasticsearch/`
   - Additional ElasticsearchAux files go in `data/elasticsearchAux/`
   - Run processing notebook to convert to Parquet

2. **AAC Data**
   - Place .zst files in `data/aac/`

### Processing Options

#### Using initialScript.py
```bash
python initialScript.py
```

This script will:
1. Create all necessary directories
2. Process Elasticsearch, ElasticsearchAux and AAC data
3. Convert to Parquet format


## 📊 Elasticsearch Analysis Examples

### Publication Year Distribution
![Publication Year Distribution](images/Publication%20Year%20Distribution.png)

Analysis of publication years across the Elasticsearch dataset.

### Language Analysis
![Language Distribution](images/Language%20Distribution.png)

Distribution of languages in the Elasticsearch collection.

### Rare vs. Non-Rare Books Analysis
![Rare vs. Non-Rare](images/Rare%20vs.%20Non-Rare.png)

Distribution between rare and non-rare books in the Elasticsearch dataset.

### AACID Analysis
![AACID Analysis](images/AACID%20Analysis.png)

AACID patterns and distribution analysis.

### Download Availability Analysis
![Download Availability](images/Download%20Availability.png)

Analysis of download options in the Elasticsearch dataset.

### Cover URL Analysis
![Cover URL Analysis](images/Cover%20URL%20Analysis.png)

Distribution of cover image availability.

### File Size Distribution by Content Type
![File Size Distribution](images/File%20Size%20Distribution.png)

File size analysis across different content types.

### Classification Analysis
![Classification Analysis](images/Classification%20Analysis.png)

Book classification distribution analysis.

### Score Analysis
![Score Analysis](images/Score%20Analysis.png)

Analysis of base rank scores in the dataset.

## 📊 ElasticsearchAux Analysis Examples

### Content Types Analysis
![Content Types Analysis](images/1Content%20Types%20Analysis.png)
Distribution of different content types across the dataset.

### Publication Year Analysis
![Publication Year Analysis](images/2Publication%20Year%20Analysis.png)
Histogram showing publication trends over different years.

### Language Distribution Analysis
![Language Distribution Analysis](images/3Language%20Distribution%20Analysis.png)
Breakdown of languages represented in the dataset.

### File Size Analysis
![File Size Analysis](images/4File%20Size%20Analysis.png)
Distribution of file sizes throughout the collection.

### Publisher Analysis
![Publisher Analysis](images/5Publisher%20Analysis.png)
Top publishers represented in the dataset by volume.

### File Extensions Analysis
![File Extensions Analysis](images/6File%20Extensions%20Analysis.png)
Frequency of different file formats in the collection.

### Access Type Analysis
![Access Type Analysis](images/7Access%20Type%20Analysis.png)
Analysis of different access methods for the content.

### Publications Over Time by Content Type
![Publications Over Time by Content Type](images/8Publications%20Over%20Time%20by%20Content%20Type.png)
Trend analysis of how content types evolved over publication years.

### Author Analysis
![Author Analysis](images/9Author%20Analysis.png)
Most represented authors in the dataset.

### Language and Publication Year Correlation
![Language and Publication Year Correlation](images/10Language%20and%20Publication%20Year%20Correlation.png)
How language distribution changes across publication years.

### Torrent Availability Analysis
![Torrent Availability Analysis](images/12Torrent%20Availability%20Analysis.png)
Analysis of torrent availability for the content.

### Title Word Cloud Analysis
![Title Word Cloud Analysis](images/14Title%20Word%20Cloud%20Analysis.png)
Visual representation of most common words in titles.

### Publication Year and File Size Correlation
![Publication Year and File Size Correlation](images/15Publication%20Year%20and%20File%20Size%20Correlation.png)
Examining how file sizes relate to publication years.

### File Extension and Content Type Analysis
![File Extension and Content Type Analysis](images/19File%20Extension%20and%20Content%20Type%20Analysis.png)
Relationship between file formats and content categories.

### Combined Dataset Comparison Overview
![Combined Dataset Comparison Overview](images/28Combined%20Dataset%20Comparison%20Overview.png)
Comparative analysis of multiple datasets in the collection.

## 📊 Anna's Archive Analysis Examples
Coming soon!

## 🚀 Quick Start

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <project-directory>
   ```

2. **Build the Docker Image**
   ```bash
   docker-compose build
   ```

3. **Start the Environment**
   ```bash
   docker-compose up
   ```

4. **Access Jupyter**
   - Open the URL shown in the console
   - Default: http://localhost:8888

## 📊 Available Notebooks

### DATA Analysis
- `Elasticsearch_Queries.ipynb`: Elasticsearch Queries
- `ElasticsearchAUX_Queries.ipynb`: ElasticsearchAUX Queries
- `AAC_Queries.ipynb`: AAC Queries

## 🔧 Configuration

### Memory Settings
- Default RAM requirement: 30GB
- DuckDB memory limit: 28GB (adjustable)
- Chunk size: 10MB (adjustable)
- Python environment: 3.12

### Performance Tuning
For systems with more resources:
- Increase chunk size for faster processing
- Adjust DuckDB memory limit
- Modify parallel processing parameters

### Docker Settings
- Port mapping: 8888:8888
- Volume mounts for data persistence
- Jupyter notebook directory mapping
