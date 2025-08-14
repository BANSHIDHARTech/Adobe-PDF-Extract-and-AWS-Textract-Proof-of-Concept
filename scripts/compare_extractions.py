"""
Compare Adobe PDF Services vs AWS Textract extraction results
This script helps you analyze the differences between the two extraction methods
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_extraction_result(filename: str) -> Dict:
    """Load extraction result from JSON file"""
    if not Path(filename).exists():
        raise FileNotFoundError(f"File not found: {filename}")
    
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def analyze_adobe_extraction(adobe_file: str) -> Dict:
    """Analyze Adobe extraction results"""
    logger.info(f"🔍 Analyzing Adobe extraction: {adobe_file}")
    
    try:
        data = load_extraction_result(adobe_file)
        
        # Count elements
        pages = data.get("pages", [])
        total_pages = len(pages)
        
        total_tables = sum(len(page.get("tables", [])) for page in pages)
        total_images = sum(len(page.get("images", [])) for page in pages)
        total_text_length = sum(len(page.get("text", "")) for page in pages)
        
        # Count tables with data
        tables_with_data = 0
        total_table_rows = 0
        total_table_cols = 0
        
        for page in pages:
            for table in page.get("tables", []):
                data_rows = table.get("data", [])
                if data_rows and len(data_rows) > 0:
                    tables_with_data += 1
                    total_table_rows += len(data_rows)
                    if data_rows:
                        max_cols = max(len(row) for row in data_rows)
                        total_table_cols += max_cols
        
        # Count images with captions
        images_with_captions = 0
        images_with_bounds = 0
        
        for page in pages:
            for image in page.get("images", []):
                if image.get("caption"):
                    images_with_captions += 1
                if image.get("bounds") or image.get("bbox"):
                    images_with_bounds += 1
        
        analysis = {
            "source": "Adobe PDF Services",
            "total_pages": total_pages,
            "total_tables": total_tables,
            "tables_with_data": tables_with_data,
            "total_images": total_images,
            "images_with_captions": images_with_captions,
            "images_with_bounds": images_with_bounds,
            "total_text_length": total_text_length,
            "table_data_rate": (tables_with_data / total_tables * 100) if total_tables > 0 else 0,
            "caption_rate": (images_with_captions / total_images * 100) if total_images > 0 else 0,
            "avg_table_rows": total_table_rows / tables_with_data if tables_with_data > 0 else 0,
            "avg_table_cols": total_table_cols / tables_with_data if tables_with_data > 0 else 0
        }
        
        logger.info(f"✅ Adobe analysis completed")
        return analysis
        
    except Exception as e:
        logger.error(f"❌ Adobe analysis failed: {e}")
        return {}

def analyze_textract_extraction(textract_file: str) -> Dict:
    """Analyze AWS Textract extraction results"""
    logger.info(f"🔍 Analyzing AWS Textract extraction: {textract_file}")
    
    try:
        data = load_extraction_result(textract_file)
        
        # Count blocks by type
        blocks = data.get("Blocks", [])
        total_blocks = len(blocks)
        
        # Count by block type
        block_types = {}
        for block in blocks:
            block_type = block.get("BlockType", "Unknown")
            block_types[block_type] = block_types.get(block_type, 0) + 1
        
        # Count pages
        pages = set(block.get("Page", 1) for block in blocks if 'Page' in block)
        total_pages = len(pages)
        
        # Count text blocks
        text_blocks = [b for b in blocks if b.get("BlockType") == "LINE"]
        total_text_length = sum(len(b.get("Text", "")) for b in text_blocks)
        
        # Count tables
        table_blocks = [b for b in blocks if b.get("BlockType") == "TABLE"]
        total_tables = len(table_blocks)
        
        # Count cells
        cell_blocks = [b for b in blocks if b.get("BlockType") == "CELL"]
        total_cells = len(cell_blocks)
        
        # Count words
        word_blocks = [b for b in blocks if b.get("BlockType") == "WORD"]
        total_words = len(word_blocks)
        
        analysis = {
            "source": "AWS Textract",
            "total_pages": total_pages,
            "total_blocks": total_blocks,
            "total_tables": total_tables,
            "total_cells": total_cells,
            "total_words": total_words,
            "total_text_length": total_text_length,
            "block_types": block_types,
            "avg_blocks_per_page": total_blocks / total_pages if total_pages > 0 else 0,
            "avg_words_per_page": total_words / total_pages if total_pages > 0 else 0
        }
        
        logger.info(f"✅ Textract analysis completed")
        return analysis
        
    except Exception as e:
        logger.error(f"❌ Textract analysis failed: {e}")
        return {}

def compare_extractions(adobe_file: str, textract_file: str) -> Dict:
    """Compare Adobe vs AWS Textract results"""
    logger.info("🔄 Comparing Adobe vs AWS Textract extractions")
    
    # Analyze both extractions
    adobe_analysis = analyze_adobe_extraction(adobe_file)
    textract_analysis = analyze_textract_extraction(textract_file)
    
    if not adobe_analysis or not textract_analysis:
        logger.error("❌ Cannot compare - one or both analyses failed")
        return {}
    
    # Calculate differences
    comparison = {
        "adobe": adobe_analysis,
        "textract": textract_analysis,
        "differences": {},
        "summary": {}
    }
    
    # Compare key metrics
    key_metrics = ["total_pages", "total_tables", "total_images", "total_text_length"]
    
    for metric in key_metrics:
        adobe_val = adobe_analysis.get(metric, 0)
        textract_val = textract_analysis.get(metric, 0)
        
        if metric in adobe_analysis and metric in textract_analysis:
            diff = adobe_val - textract_val
            diff_percent = (diff / adobe_val * 100) if adobe_val > 0 else 0
            
            comparison["differences"][metric] = {
                "adobe": adobe_val,
                "textract": textract_val,
                "difference": diff,
                "difference_percent": diff_percent
            }
    
    # Generate summary
    comparison["summary"] = {
        "adobe_advantages": [],
        "textract_advantages": [],
        "overall_assessment": ""
    }
    
    # Analyze advantages
    if adobe_analysis.get("total_tables", 0) > textract_analysis.get("total_tables", 0):
        comparison["summary"]["adobe_advantages"].append("Better table detection")
    
    if adobe_analysis.get("total_images", 0) > textract_analysis.get("total_images", 0):
        comparison["summary"]["adobe_advantages"].append("Better image detection")
    
    if textract_analysis.get("total_words", 0) > adobe_analysis.get("total_text_length", 0) / 5:  # Rough word count
        comparison["summary"]["textract_advantages"].append("Better word-level text extraction")
    
    # Overall assessment
    adobe_score = len(comparison["summary"]["adobe_advantages"])
    textract_score = len(comparison["summary"]["textract_advantages"])
    
    if adobe_score > textract_score:
        comparison["summary"]["overall_assessment"] = "Adobe performs better for this document"
    elif textract_score > adobe_score:
        comparison["summary"]["overall_assessment"] = "AWS Textract performs better for this document"
    else:
        comparison["summary"]["overall_assessment"] = "Both perform similarly for this document"
    
    return comparison

def save_comparison_report(comparison: Dict, output_file: str = "outputs/extraction_comparison.json"):
    """Save comparison report to file"""
    try:
        # Create outputs directory if it doesn't exist
        Path(output_file).parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(comparison, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Comparison report saved to: {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"❌ Failed to save comparison report: {e}")
        return None

def display_comparison_summary(comparison: Dict):
    """Display a summary of the comparison"""
    if not comparison:
        logger.error("❌ No comparison data to display")
        return
    
    logger.info("📊 EXTRACTION COMPARISON SUMMARY")
    logger.info("=" * 60)
    
    # Adobe results
    adobe = comparison.get("adobe", {})
    logger.info(f"🔵 ADOBE PDF SERVICES:")
    logger.info(f"   Pages: {adobe.get('total_pages', 0)}")
    logger.info(f"   Tables: {adobe.get('total_tables', 0)} (with data: {adobe.get('tables_with_data', 0)})")
    logger.info(f"   Images: {adobe.get('total_images', 0)} (with captions: {adobe.get('images_with_captions', 0)})")
    logger.info(f"   Text length: {adobe.get('total_text_length', 0)} chars")
    
    # Textract results
    textract = comparison.get("textract", {})
    logger.info(f"🟠 AWS TEXTRACT:")
    logger.info(f"   Pages: {textract.get('total_pages', 0)}")
    logger.info(f"   Tables: {textract.get('total_tables', 0)}")
    logger.info(f"   Total blocks: {textract.get('total_blocks', 0)}")
    logger.info(f"   Words: {textract.get('total_words', 0)}")
    logger.info(f"   Text length: {textract.get('total_text_length', 0)} chars")
    
    # Differences
    logger.info(f"\n📈 KEY DIFFERENCES:")
    differences = comparison.get("differences", {})
    for metric, diff_data in differences.items():
        adobe_val = diff_data["adobe"]
        textract_val = diff_data["textract"]
        diff_percent = diff_data["difference_percent"]
        
        if abs(diff_percent) > 5:  # Only show significant differences
            logger.info(f"   {metric}: Adobe={adobe_val}, Textract={textract_val} (diff: {diff_percent:+.1f}%)")
    
    # Summary
    summary = comparison.get("summary", {})
    logger.info(f"\n🎯 OVERALL ASSESSMENT:")
    logger.info(f"   {summary.get('overall_assessment', 'Unable to assess')}")
    
    if summary.get("adobe_advantages"):
        logger.info(f"   Adobe advantages: {', '.join(summary['adobe_advantages'])}")
    
    if summary.get("textract_advantages"):
        logger.info(f"   Textract advantages: {', '.join(summary['textract_advantages'])}")

def main():
    """Main function"""
    logger.info("🚀 ADOBE vs AWS TEXTRACT COMPARISON")
    logger.info("=" * 60)
    
    # Look for extraction files
    output_dir = Path("outputs")
    
    # Find Adobe extraction file
    adobe_files = list(output_dir.glob("*_adobe_restructured.json"))
    if not adobe_files:
        logger.error("❌ No Adobe extraction files found. Run Adobe extraction first!")
        return 1
    
    adobe_file = str(adobe_files[0])
    logger.info(f"📁 Adobe file: {adobe_file}")
    
    # Find Textract extraction file
    textract_files = list(output_dir.glob("*textract_extraction.json"))
    if not textract_files:
        logger.error("❌ No Textract extraction files found. Run Textract extraction first!")
        logger.info("💡 Command: python scripts/aws_textract_poc_fixed.py --input inputs\\bray_sample.pdf --bucket my-textract-poc-25 --key poc/bray_sample.pdf --out outputs\\textract_extraction.json")
        return 1
    
    textract_file = str(textract_files[0])
    logger.info(f"📁 Textract file: {textract_file}")
    
    # Compare extractions
    comparison = compare_extractions(adobe_file, textract_file)
    
    if comparison:
        # Display summary
        display_comparison_summary(comparison)
        
        # Save report
        output_file = save_comparison_report(comparison)
        if output_file:
            logger.info(f"\n📁 Detailed comparison saved to: {output_file}")
        
        return 0
    else:
        logger.error("❌ Comparison failed")
        return 1

if __name__ == '__main__':
    exit(main())
