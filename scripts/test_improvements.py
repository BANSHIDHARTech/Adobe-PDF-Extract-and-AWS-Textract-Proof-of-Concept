"""
Test script for the improved PDF extraction system.
This script demonstrates the new features and validates the improvements.
"""
import json
import logging
from pathlib import Path
from adobe_extract_improved import AdobePDFExtractor
from verify_extraction import ExtractionVerifier

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_automatic_folder_detection():
    """Test automatic folder detection functionality"""
    logger.info("🧪 Testing automatic folder detection...")
    
    try:
        # Create extractor with default directories
        extractor = AdobePDFExtractor()
        
        # Check if directories exist
        assert extractor.input_dir.exists(), f"Input directory {extractor.input_dir} should exist"
        assert extractor.output_dir.exists(), f"Output directory {extractor.output_dir} should exist"
        
        # Check if PDF files are found
        pdf_files = extractor._find_pdf_files()
        logger.info(f"Found {len(pdf_files)} PDF file(s): {[f.name for f in pdf_files]}")
        
        if pdf_files:
            logger.info("✅ Automatic folder detection working correctly")
            return True
        else:
            logger.warning("⚠️ No PDF files found in inputs directory")
            return False
            
    except Exception as e:
        logger.error(f"❌ Automatic folder detection test failed: {e}")
        return False

def test_credentials_detection():
    """Test Adobe credentials detection"""
    logger.info("🔑 Testing credentials detection...")
    
    try:
        extractor = AdobePDFExtractor()
        
        # Try to find credentials
        creds_path = extractor._find_credentials()
        logger.info(f"Credentials found at: {creds_path}")
        
        # Try to extract credentials
        client_id, client_secret = extractor._extract_credentials(creds_path)
        logger.info(f"Client ID: {client_id[:8]}...")
        logger.info(f"Client Secret: {'*' * min(8, len(client_secret))}...")
        
        logger.info("✅ Credentials detection working correctly")
        return True
        
    except FileNotFoundError:
        logger.warning("⚠️ Adobe credentials file not found - this is expected if credentials aren't set up")
        return False
    except Exception as e:
        logger.error(f"❌ Credentials detection test failed: {e}")
        return False

def test_output_structure_validation():
    """Test the new output structure validation"""
    logger.info("📋 Testing output structure validation...")
    
    try:
        verifier = ExtractionVerifier()
        
        # Check if there are any restructured files to test
        output_dir = Path("outputs")
        restructured_files = list(output_dir.glob("*_adobe_restructured.json"))
        
        if not restructured_files:
            logger.warning("⚠️ No restructured files found to test - run extraction first")
            return False
        
        # Test the first restructured file
        test_file = restructured_files[0]
        logger.info(f"Testing file: {test_file.name}")
        
        # Generate verification report - use the full path
        report = verifier.generate_verification_report(str(test_file), include_comparison=True)
        
        if report["verification_complete"]:
            logger.info(f"✅ Structure validation successful")
            logger.info(f"Overall assessment: {report['overall_assessment']}")
            
            # Display summary
            summary = report["content_verification"]["content_summary"]
            # Get page count from metadata instead of content summary
            page_count = report["structure_verification"]["structure_summary"]["total_pages"]
            logger.info(f"Content summary: {page_count} pages, {summary['total_tables']} tables, {summary['total_images']} images")
            
            return True
        else:
            logger.error(f"❌ Structure validation failed: {report.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Output structure validation test failed: {e}")
        return False

def test_image_extraction_analysis():
    """Test image extraction analysis from existing data"""
    logger.info("🖼️ Testing image extraction analysis...")
    
    try:
        # Load existing extraction data to analyze image handling
        output_dir = Path("outputs")
        restructured_files = list(output_dir.glob("*_adobe_restructured.json"))
        
        if not restructured_files:
            logger.warning("⚠️ No restructured files found to analyze images")
            return False
        
        # Analyze the first file
        test_file = restructured_files[0]
        with open(test_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Count images across all pages
        total_images = 0
        images_with_captions = 0
        images_with_bounds = 0
        
        for page in data.get("pages", []):
            images = page.get("images", [])
            total_images += len(images)
            
            for image in images:
                if image.get("caption"):
                    images_with_captions += 1
                if image.get("bounds") or image.get("bbox"):
                    images_with_bounds += 1
        
        logger.info(f"📊 Image Analysis Results:")
        logger.info(f"   Total images: {total_images}")
        logger.info(f"   Images with captions: {images_with_captions}")
        logger.info(f"   Images with bounds: {images_with_bounds}")
        
        if total_images > 0:
            caption_rate = (images_with_captions / total_images) * 100
            bounds_rate = (images_with_bounds / total_images) * 100
            logger.info(f"   Caption extraction rate: {caption_rate:.1f}%")
            logger.info(f"   Bounds extraction rate: {bounds_rate:.1f}%")
            
            logger.info("✅ Image extraction analysis completed")
            return True
        else:
            logger.info("ℹ️ No images found in the test document")
            return True
            
    except Exception as e:
        logger.error(f"❌ Image extraction analysis test failed: {e}")
        return False

def test_table_extraction_analysis():
    """Test table extraction analysis from existing data"""
    logger.info("📊 Testing table extraction analysis...")
    
    try:
        # Load existing extraction data to analyze table handling
        output_dir = Path("outputs")
        restructured_files = list(output_dir.glob("*_adobe_restructured.json"))
        
        if not restructured_files:
            logger.warning("⚠️ No restructured files found to analyze tables")
            return False
        
        # Analyze the first file
        test_file = restructured_files[0]
        with open(test_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Count tables across all pages
        total_tables = 0
        tables_with_data = 0
        total_rows = 0
        total_columns = 0
        
        for page in data.get("pages", []):
            tables = page.get("tables", [])
            total_tables += len(tables)
            
            for table in tables:
                data_rows = table.get("data", [])
                if data_rows and len(data_rows) > 0:
                    tables_with_data += 1
                    total_rows += len(data_rows)
                    if data_rows:
                        max_cols = max(len(row) for row in data_rows)
                        total_columns += max_cols
        
        logger.info(f"📊 Table Analysis Results:")
        logger.info(f"   Total tables: {total_tables}")
        logger.info(f"   Tables with data: {tables_with_data}")
        
        if total_tables > 0:
            data_rate = (tables_with_data / total_tables) * 100
            logger.info(f"   Data extraction rate: {data_rate:.1f}%")
            
            if tables_with_data > 0:
                avg_rows = total_rows / tables_with_data
                avg_cols = total_columns / tables_with_data
                logger.info(f"   Average rows per table: {avg_rows:.1f}")
                logger.info(f"   Average columns per table: {avg_cols:.1f}")
            
            logger.info("✅ Table extraction analysis completed")
            return True
        else:
            logger.info("ℹ️ No tables found in the test document")
            return True
            
    except Exception as e:
        logger.error(f"❌ Table extraction analysis test failed: {e}")
        return False

def test_output_format_compliance():
    """Test if output format matches the specified structure"""
    logger.info("✅ Testing output format compliance...")
    
    try:
        # Load existing extraction data
        output_dir = Path("outputs")
        restructured_files = list(output_dir.glob("*_adobe_restructured.json"))
        
        if not restructured_files:
            logger.warning("⚠️ No restructured files found to test format compliance")
            return False
        
        # Test the first file
        test_file = restructured_files[0]
        with open(test_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check required top-level keys
        required_keys = ["document_id", "metadata", "pages"]
        missing_keys = [key for key in required_keys if key not in data]
        
        if missing_keys:
            logger.error(f"❌ Missing required keys: {missing_keys}")
            return False
        
        # Check metadata structure
        metadata = data.get("metadata", {})
        required_metadata = ["source", "page_count"]
        missing_metadata = [key for key in required_metadata if key not in metadata]
        
        if missing_metadata:
            logger.error(f"❌ Missing metadata keys: {missing_metadata}")
            return False
        
        # Check pages structure
        pages = data.get("pages", [])
        expected_pages = metadata.get("page_count", 0)
        
        if len(pages) != expected_pages:
            logger.error(f"❌ Page count mismatch: expected {expected_pages}, got {len(pages)}")
            return False
        
        # Check each page structure
        for i, page in enumerate(pages):
            page_num = i + 1
            required_page_keys = ["page_number", "text", "tables", "images"]
            
            missing_page_keys = [key for key in required_page_keys if key not in page]
            if missing_page_keys:
                logger.error(f"❌ Page {page_num}: Missing keys: {missing_page_keys}")
                return False
            
            # Check page number consistency
            if page.get("page_number") != page_num:
                logger.error(f"❌ Page {page_num}: Page number mismatch")
                return False
        
        logger.info("✅ Output format compliance test passed")
        logger.info(f"   Document ID: {data.get('document_id')}")
        logger.info(f"   Source: {metadata.get('source')}")
        logger.info(f"   Pages: {len(pages)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Output format compliance test failed: {e}")
        return False

def run_all_tests():
    """Run all tests and provide summary"""
    logger.info("🚀 Starting comprehensive test suite...")
    
    tests = [
        ("Automatic Folder Detection", test_automatic_folder_detection),
        ("Credentials Detection", test_credentials_detection),
        ("Output Structure Validation", test_output_structure_validation),
        ("Image Extraction Analysis", test_image_extraction_analysis),
        ("Table Extraction Analysis", test_table_extraction_analysis),
        ("Output Format Compliance", test_output_format_compliance)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            result = test_func()
            results.append((test_name, result))
            
            if result:
                logger.info(f"✅ {test_name}: PASSED")
            else:
                logger.warning(f"⚠️ {test_name}: PARTIAL/FAILED")
                
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*50}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED/ERROR"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        logger.info("🎉 All tests passed! The improved system is working correctly.")
    elif passed >= total * 0.8:
        logger.info("👍 Most tests passed. The system is working well with minor issues.")
    else:
        logger.warning("⚠️ Several tests failed. Review the issues above.")
    
    return passed == total

if __name__ == '__main__':
    success = run_all_tests()
    exit(0 if success else 1)
