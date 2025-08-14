"""
Batch processing script for PDF extraction.
Automatically processes all PDFs in the inputs folder and generates comprehensive reports.
"""
import os
import json
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any
from adobe_extract_improved import AdobePDFExtractor
from verify_extraction import ExtractionVerifier

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, input_dir: str = "inputs", output_dir: str = "outputs"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.extractor = AdobePDFExtractor(input_dir, output_dir)
        self.verifier = ExtractionVerifier(output_dir)
    
    def process_all_pdfs(self, verify_results: bool = True) -> Dict[str, Any]:
        """Process all PDFs and optionally verify results"""
        logger.info("🚀 Starting batch PDF processing")
        
        # Find all PDF files
        pdf_files = list(self.input_dir.glob("*.pdf"))
        if not pdf_files:
            logger.warning(f"No PDF files found in {self.input_dir}")
            return {"status": "no_files", "processed": 0}
        
        logger.info(f"Found {len(pdf_files)} PDF file(s) to process")
        
        results = {
            "status": "processing",
            "total_files": len(pdf_files),
            "processed_files": 0,
            "failed_files": 0,
            "results": [],
            "summary": {}
        }
        
        # Process each PDF
        for pdf_file in pdf_files:
            try:
                logger.info(f"📄 Processing: {pdf_file.name}")
                
                # Extract content
                extraction_result = self.extractor.extract_pdf(pdf_file)
                
                # Verify if requested
                if verify_results:
                    verification = self.extractor.verify_extraction(pdf_file, extraction_result)
                    
                    # Save verification results
                    verification_file = self.output_dir / f"{pdf_file.stem}_verification.json"
                    with open(verification_file, 'w', encoding='utf-8') as f:
                        json.dump(verification, f, indent=2)
                    
                    # Add verification info to result
                    extraction_result["verification"] = verification
                    
                    if verification["verification_passed"]:
                        logger.info(f"✅ {pdf_file.name}: Extraction and verification successful")
                    else:
                        logger.warning(f"⚠️ {pdf_file.name}: Verification found issues")
                        for issue in verification["issues"]:
                            logger.warning(f"   - {issue}")
                
                results["results"].append({
                    "filename": pdf_file.name,
                    "status": "success",
                    "extraction": extraction_result,
                    "verification_passed": extraction_result.get("verification", {}).get("verification_passed", False) if verify_results else None
                })
                
                results["processed_files"] += 1
                
            except Exception as e:
                logger.error(f"❌ Failed to process {pdf_file.name}: {str(e)}")
                results["results"].append({
                    "filename": pdf_file.name,
                    "status": "failed",
                    "error": str(e)
                })
                results["failed_files"] += 1
        
        # Generate summary
        results["summary"] = self._generate_summary(results["results"])
        
        # Save batch results
        batch_results_file = self.output_dir / "batch_processing_results.json"
        with open(batch_results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📁 Batch results saved to: {batch_results_file}")
        
        # Final status
        if results["failed_files"] == 0:
            results["status"] = "completed"
            logger.info("🎉 All PDFs processed successfully!")
        else:
            results["status"] = "completed_with_errors"
            logger.warning(f"⚠️ Processing completed with {results['failed_files']} error(s)")
        
        return results
    
    def _generate_summary(self, results: List[Dict]) -> Dict[str, Any]:
        """Generate summary statistics from processing results"""
        successful_results = [r for r in results if r["status"] == "success"]
        failed_results = [r for r in results if r["status"] == "failed"]
        
        summary = {
            "total_files": len(results),
            "successful": len(successful_results),
            "failed": len(failed_results),
            "success_rate": len(successful_results) / len(results) if results else 0,
            "content_summary": {}
        }
        
        if successful_results:
            # Aggregate content statistics
            total_pages = 0
            total_tables = 0
            total_images = 0
            total_text_length = 0
            verification_passed = 0
            
            for result in successful_results:
                extraction = result.get("extraction", {})
                pages = extraction.get("pages", [])
                
                total_pages += len(pages)
                total_tables += sum(len(page.get("tables", [])) for page in pages)
                total_images += sum(len(page.get("images", [])) for page in pages)
                total_text_length += sum(len(page.get("text", "")) for page in pages)
                
                if result.get("verification_passed"):
                    verification_passed += 1
            
            summary["content_summary"] = {
                "total_pages": total_pages,
                "total_tables": total_tables,
                "total_images": total_images,
                "total_text_length": total_text_length,
                "average_pages_per_file": total_pages / len(successful_results) if successful_results else 0,
                "average_tables_per_file": total_tables / len(successful_results) if successful_results else 0,
                "average_images_per_file": total_images / len(successful_results) if successful_results else 0,
                "verification_passed": verification_passed,
                "verification_rate": verification_passed / len(successful_results) if successful_results else 0
            }
        
        return summary
    
    def generate_comprehensive_report(self, batch_results: Dict) -> Dict[str, Any]:
        """Generate a comprehensive report with detailed analysis"""
        report = {
            "report_type": "comprehensive_batch_analysis",
            "timestamp": str(Path().cwd()),
            "batch_summary": batch_results["summary"],
            "file_analysis": [],
            "recommendations": []
        }
        
        # Analyze each file
        for result in batch_results["results"]:
            if result["status"] == "success":
                extraction = result["extraction"]
                verification = extraction.get("verification", {})
                
                file_analysis = {
                    "filename": result["filename"],
                    "page_count": extraction.get("metadata", {}).get("page_count", 0),
                    "content_analysis": {
                        "text_pages": sum(1 for page in extraction.get("pages", []) if page.get("text")),
                        "table_pages": sum(1 for page in extraction.get("pages", []) if page.get("tables")),
                        "image_pages": sum(1 for page in extraction.get("pages", []) if page.get("images")),
                        "total_tables": sum(len(page.get("tables", [])) for page in extraction.get("pages", [])),
                        "total_images": sum(len(page.get("images", [])) for page in extraction.get("pages", [])),
                        "total_text_length": sum(len(page.get("text", "")) for page in extraction.get("pages", []))
                    },
                    "verification": {
                        "passed": verification.get("verification_passed", False),
                        "issues": verification.get("issues", []),
                        "summary": verification.get("summary", {})
                    }
                }
                
                report["file_analysis"].append(file_analysis)
            else:
                report["file_analysis"].append({
                    "filename": result["filename"],
                    "status": "failed",
                    "error": result.get("error", "Unknown error")
                })
        
        # Generate recommendations
        if batch_results["summary"]["failed"] > 0:
            report["recommendations"].append("Review failed extractions and check Adobe credentials")
        
        if batch_results["summary"]["success_rate"] < 0.8:
            report["recommendations"].append("Success rate below 80%. Consider reviewing PDF quality and extraction parameters")
        
        # Check for common issues
        verification_issues = []
        for analysis in report["file_analysis"]:
            if analysis.get("verification", {}).get("issues"):
                verification_issues.extend(analysis["verification"]["issues"])
        
        if verification_issues:
            common_issues = {}
            for issue in verification_issues:
                common_issues[issue] = common_issues.get(issue, 0) + 1
            
            report["recommendations"].append(f"Common verification issues: {dict(sorted(common_issues.items(), key=lambda x: x[1], reverse=True))}")
        
        return report
    
    def save_comprehensive_report(self, report: Dict, filename: str = "comprehensive_batch_report.json") -> Path:
        """Save comprehensive report to file"""
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Comprehensive report saved to: {output_path}")
        return output_path

def main():
    parser = argparse.ArgumentParser(description='Batch process PDFs with automatic verification')
    parser.add_argument('--input-dir', default='inputs', help='Input directory containing PDF files')
    parser.add_argument('--output-dir', default='outputs', help='Output directory for results')
    parser.add_argument('--no-verify', action='store_true', help='Skip verification step')
    parser.add_argument('--comprehensive-report', action='store_true', help='Generate comprehensive analysis report')
    
    args = parser.parse_args()
    
    try:
        processor = BatchProcessor(args.input_dir, args.output_dir)
        
        # Process all PDFs
        batch_results = processor.process_all_pdfs(verify_results=not args.no_verify)
        
        # Display summary
        logger.info("📊 Batch Processing Summary")
        logger.info(f"Total files: {batch_results['summary']['total_files']}")
        logger.info(f"Successful: {batch_results['summary']['successful']}")
        logger.info(f"Failed: {batch_results['summary']['failed']}")
        logger.info(f"Success rate: {batch_results['summary']['success_rate']:.1%}")
        
        if batch_results['summary']['content_summary']:
            content = batch_results['summary']['content_summary']
            logger.info(f"Total pages: {content['total_pages']}")
            logger.info(f"Total tables: {content['total_tables']}")
            logger.info(f"Total images: {content['total_images']}")
            logger.info(f"Verification passed: {content['verification_passed']}/{batch_results['summary']['successful']}")
        
        # Generate comprehensive report if requested
        if args.comprehensive_report:
            comprehensive_report = processor.generate_comprehensive_report(batch_results)
            processor.save_comprehensive_report(comprehensive_report)
        
        return 0 if batch_results["status"] in ["completed", "completed_with_errors"] else 1
        
    except Exception as e:
        logger.error(f"❌ Batch processing failed: {str(e)}")
        return 1

if __name__ == '__main__':
    exit(main())
