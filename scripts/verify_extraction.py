"""
Verification script for PDF extraction results.
This script helps validate that the extracted content matches the actual PDF.
"""
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExtractionVerifier:
    def __init__(self, output_dir: str = "outputs"):
        self.output_dir = Path(output_dir)
    
    def load_extraction_result(self, filename: str) -> Dict:
        """Load extraction result from JSON file"""
        # Handle both relative and absolute paths
        if Path(filename).is_absolute() or filename.startswith(('outputs/', 'outputs\\')):
            file_path = Path(filename)
        else:
            file_path = self.output_dir / filename
            
        if not file_path.exists():
            raise FileNotFoundError(f"Extraction result file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def verify_structure(self, data: Dict) -> Dict:
        """Verify the structure of the extraction result"""
        verification = {
            "structure_valid": True,
            "issues": [],
            "structure_summary": {}
        }
        
        # Check required top-level keys
        required_keys = ["document_id", "metadata", "pages"]
        for key in required_keys:
            if key not in data:
                verification["structure_valid"] = False
                verification["issues"].append(f"Missing required key: {key}")
        
        if not verification["structure_valid"]:
            return verification
        
        # Check metadata structure
        metadata = data.get("metadata", {})
        required_metadata = ["source", "page_count"]
        for key in required_metadata:
            if key not in metadata:
                verification["issues"].append(f"Missing metadata key: {key}")
        
        # Check pages structure
        pages = data.get("pages", [])
        expected_pages = metadata.get("page_count", 0)
        
        if len(pages) != expected_pages:
            verification["issues"].append(f"Page count mismatch: expected {expected_pages}, got {len(pages)}")
        
        # Verify each page structure
        for i, page in enumerate(pages):
            page_num = i + 1
            required_page_keys = ["page_number", "text", "tables", "images"]
            
            for key in required_page_keys:
                if key not in page:
                    verification["issues"].append(f"Page {page_num}: Missing required key: {key}")
            
            # Check page number consistency
            if page.get("page_number") != page_num:
                verification["issues"].append(f"Page {page_num}: Page number mismatch: expected {page_num}, got {page.get('page_number')}")
            
            # Check tables structure
            tables = page.get("tables", [])
            for j, table in enumerate(tables):
                table_id = f"t{j+1}"
                if not table.get("table_id"):
                    verification["issues"].append(f"Page {page_num}: Table {table_id} missing table_id")
                if not table.get("data"):
                    verification["issues"].append(f"Page {page_num}: Table {table_id} missing data")
            
            # Check images structure
            images = page.get("images", [])
            for j, image in enumerate(images):
                image_id = f"i{j+1}"
                if not image.get("image_id"):
                    verification["issues"].append(f"Page {page_num}: Image {image_id} missing image_id")
                if not image.get("path"):
                    verification["issues"].append(f"Page {page_num}: Image {image_id} missing path")
        
        # Summary statistics
        verification["structure_summary"] = {
            "total_pages": len(pages),
            "total_tables": sum(len(page.get("tables", [])) for page in pages),
            "total_images": sum(len(page.get("images", [])) for page in pages),
            "total_text_length": sum(len(page.get("text", "")) for page in pages),
            "pages_with_content": sum(1 for page in pages if page.get("text") or page.get("tables") or page.get("images"))
        }
        
        if verification["issues"]:
            verification["structure_valid"] = False
        
        return verification
    
    def verify_content_quality(self, data: Dict) -> Dict:
        """Verify the quality of extracted content"""
        verification = {
            "content_quality": "good",
            "warnings": [],
            "content_summary": {}
        }
        
        pages = data.get("pages", [])
        
        # Check for empty pages
        empty_pages = []
        for i, page in enumerate(pages):
            page_num = i + 1
            text = page.get("text", "")
            tables = page.get("tables", [])
            images = page.get("images", [])
            
            if not text and not tables and not images:
                empty_pages.append(page_num)
        
        if empty_pages:
            verification["warnings"].append(f"Empty pages detected: {empty_pages}")
            verification["content_quality"] = "warning"
        
        # Check text quality
        text_stats = []
        for i, page in enumerate(pages):
            page_num = i + 1
            text = page.get("text", "")
            if text:
                text_stats.append({
                    "page": page_num,
                    "length": len(text),
                    "word_count": len(text.split()),
                    "has_content": bool(text.strip())
                })
        
        # Check table quality
        table_stats = []
        for i, page in enumerate(pages):
            page_num = i + 1
            tables = page.get("tables", [])
            for j, table in enumerate(tables):
                table_id = table.get("table_id", f"t{j+1}")
                data = table.get("data", [])
                if data:
                    table_stats.append({
                        "page": page_num,
                        "table_id": table_id,
                        "rows": len(data),
                        "columns": max(len(row) for row in data) if data else 0,
                        "has_data": bool(data and any(any(cell.strip() for cell in row) for row in data))
                    })
        
        # Check image quality
        image_stats = []
        for i, page in enumerate(pages):
            page_num = i + 1
            images = page.get("images", [])
            for j, image in enumerate(images):
                image_id = image.get("image_id", f"i{j+1}")
                caption = image.get("caption")
                bounds = image.get("bounds") or image.get("bbox", [])
                
                image_stats.append({
                    "page": page_num,
                    "image_id": image_id,
                    "has_caption": bool(caption),
                    "has_bounds": len(bounds) >= 4,
                    "caption_length": len(caption) if caption else 0
                })
        
        # Content summary
        verification["content_summary"] = {
            "text_pages": len([p for p in text_stats if p["has_content"]]),
            "table_pages": len(set(t["page"] for t in table_stats)),
            "image_pages": len(set(i["page"] for i in image_stats)),
            "total_tables": len(table_stats),
            "total_images": len(image_stats),
            "tables_with_data": sum(1 for t in table_stats if t["has_data"]),
            "images_with_captions": sum(1 for i in image_stats if i["has_caption"]),
            "images_with_bounds": sum(1 for i in image_stats if i["has_bounds"])
        }
        
        # Quality assessment
        if empty_pages:
            verification["content_quality"] = "warning"
        
        if verification["content_summary"]["tables_with_data"] < verification["content_summary"]["total_tables"]:
            verification["warnings"].append("Some tables have no data")
            verification["content_quality"] = "warning"
        
        if verification["content_summary"]["images_with_bounds"] < verification["content_summary"]["total_images"]:
            verification["warnings"].append("Some images missing bounds")
            verification["content_quality"] = "warning"
        
        return verification
    
    def compare_with_original(self, restructured_file: str, original_file: str) -> Dict:
        """Compare restructured output with original Adobe output"""
        try:
            restructured = self.load_extraction_result(restructured_file)
            original = self.load_extraction_result(original_file)
        except FileNotFoundError as e:
            return {"comparison_valid": False, "error": str(e)}
        
        comparison = {
            "comparison_valid": True,
            "differences": [],
            "summary": {}
        }
        
        # Compare page counts
        restructured_pages = restructured.get("metadata", {}).get("page_count", 0)
        original_pages = original.get("extended_metadata", {}).get("page_count", 0)
        
        if restructured_pages != original_pages:
            comparison["differences"].append(f"Page count: restructured={restructured_pages}, original={original_pages}")
        
        # Compare element counts
        original_elements = original.get("elements", [])
        element_counts = {}
        for element in original_elements:
            path = element.get("Path", "")
            if path.startswith("//Document/"):
                element_type = path.split("/")[-1]
                element_counts[element_type] = element_counts.get(element_type, 0) + 1
        
        # Count in restructured
        restructured_counts = {
            "tables": sum(len(page.get("tables", [])) for page in restructured.get("pages", [])),
            "images": sum(len(page.get("images", [])) for page in restructured.get("pages", [])),
            "text": sum(1 for page in restructured.get("pages", []) if page.get("text"))
        }
        
        # Compare counts
        for elem_type, count in element_counts.items():
            if elem_type == "Table" and count != restructured_counts["tables"]:
                comparison["differences"].append(f"Table count: restructured={restructured_counts['tables']}, original={count}")
            elif elem_type == "Figure" and count != restructured_counts["images"]:
                comparison["differences"].append(f"Image count: restructured={restructured_counts['images']}, original={count}")
        
        comparison["summary"] = {
            "original_elements": element_counts,
            "restructured_elements": restructured_counts,
            "page_count_match": restructured_pages == original_pages
        }
        
        if comparison["differences"]:
            comparison["comparison_valid"] = False
        
        return comparison
    
    def generate_verification_report(self, filename: str, include_comparison: bool = False) -> Dict:
        """Generate a comprehensive verification report"""
        try:
            data = self.load_extraction_result(filename)
        except FileNotFoundError as e:
            return {"verification_complete": False, "error": str(e)}
        
        report = {
            "verification_complete": True,
            "filename": filename,
            "timestamp": str(Path(filename).stat().st_mtime),
            "structure_verification": self.verify_structure(data),
            "content_verification": self.verify_content_quality(data)
        }
        
        # Add comparison if requested and original file exists
        if include_comparison:
            original_file = filename.replace("_restructured", "_original")
            if (self.output_dir / original_file).exists():
                report["comparison"] = self.compare_with_original(filename, original_file)
        
        # Overall assessment
        structure_valid = report["structure_verification"]["structure_valid"]
        content_quality = report["content_verification"]["content_quality"]
        
        if structure_valid and content_quality == "good":
            report["overall_assessment"] = "excellent"
        elif structure_valid and content_quality == "warning":
            report["overall_assessment"] = "good"
        elif not structure_valid:
            report["overall_assessment"] = "poor"
        else:
            report["overall_assessment"] = "fair"
        
        return report
    
    def save_verification_report(self, report: Dict, output_filename: str = None) -> Path:
        """Save verification report to file"""
        if output_filename is None:
            input_filename = Path(report["filename"]).stem
            output_filename = f"{input_filename}_verification_report.json"
        
        output_path = self.output_dir / output_filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Verification report saved to: {output_path}")
        return output_path

def main():
    parser = argparse.ArgumentParser(description='Verify PDF extraction results')
    parser.add_argument('--file', required=True, help='Extraction result file to verify')
    parser.add_argument('--output-dir', default='outputs', help='Output directory')
    parser.add_argument('--compare', action='store_true', help='Compare with original Adobe output')
    parser.add_argument('--save-report', action='store_true', help='Save verification report to file')
    
    args = parser.parse_args()
    
    verifier = ExtractionVerifier(args.output_dir)
    
    try:
        # Generate verification report
        report = verifier.generate_verification_report(args.file, args.compare)
        
        if not report["verification_complete"]:
            logger.error(f"Verification failed: {report['error']}")
            return 1
        
        # Display results
        logger.info("🔍 Verification Report")
        logger.info(f"File: {report['filename']}")
        logger.info(f"Overall Assessment: {report['overall_assessment'].upper()}")
        
        # Structure verification
        structure = report["structure_verification"]
        if structure["structure_valid"]:
            logger.info("✅ Structure: Valid")
        else:
            logger.error("❌ Structure: Invalid")
            for issue in structure["issues"]:
                logger.error(f"   - {issue}")
        
        # Content verification
        content = report["content_verification"]
        logger.info(f"📊 Content Quality: {content['content_quality'].upper()}")
        if content["warnings"]:
            for warning in content["warnings"]:
                logger.warning(f"   ⚠️ {warning}")
        
        # Summary statistics
        summary = content["content_summary"]
        logger.info("📈 Content Summary:")
        logger.info(f"   Text pages: {summary['text_pages']}")
        logger.info(f"   Table pages: {summary['table_pages']}")
        logger.info(f"   Image pages: {summary['image_pages']}")
        logger.info(f"   Total tables: {summary['total_tables']}")
        logger.info(f"   Total images: {summary['total_images']}")
        logger.info(f"   Tables with data: {summary['tables_with_data']}")
        logger.info(f"   Images with captions: {summary['images_with_captions']}")
        
        # Comparison results
        if "comparison" in report:
            comparison = report["comparison"]
            if comparison["comparison_valid"]:
                logger.info("✅ Comparison: Valid")
            else:
                logger.warning("⚠️ Comparison: Differences found")
                for diff in comparison["differences"]:
                    logger.warning(f"   - {diff}")
        
        # Save report if requested
        if args.save_report:
            output_path = verifier.save_verification_report(report)
            logger.info(f"📁 Report saved to: {output_path}")
        
        return 0 if report["overall_assessment"] in ["excellent", "good"] else 1
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        return 1

if __name__ == '__main__':
    exit(main())
