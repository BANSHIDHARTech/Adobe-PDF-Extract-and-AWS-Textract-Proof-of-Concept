#!/usr/bin/env python3
"""
Simple verification script for PDF extraction results.
"""
import json
import sys
from pathlib import Path

def verify_json_structure(file_path):
    """Verify the basic structure of the JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print("🔍 Verification Report")
        print(f"File: {file_path}")
        print()
        
        # Check top-level structure
        required_keys = ["document_id", "metadata", "pages"]
        missing_keys = [key for key in required_keys if key not in data]
        
        if missing_keys:
            print(f"❌ Missing required keys: {missing_keys}")
            return False
        else:
            print("✅ All required top-level keys present")
        
        # Check metadata
        metadata = data.get("metadata", {})
        print(f"📊 Metadata: {metadata}")
        
        # Check pages
        pages = data.get("pages", [])
        print(f"📄 Total pages: {len(pages)}")
        
        if pages:
            first_page = pages[0]
            page_keys = list(first_page.keys())
            print(f"📋 First page keys: {page_keys}")
            
            # Check page structure
            required_page_keys = ["page_number", "text", "tables", "images"]
            missing_page_keys = [key for key in required_page_keys if key not in first_page]
            
            if missing_page_keys:
                print(f"❌ Missing page keys: {missing_page_keys}")
            else:
                print("✅ All required page keys present")
            
            # Count content
            total_tables = sum(len(page.get("tables", [])) for page in pages)
            total_images = sum(len(page.get("images", [])) for page in pages)
            total_text_length = sum(len(page.get("text", "")) for page in pages)
            
            print(f"📊 Content Summary:")
            print(f"   Total tables: {total_tables}")
            print(f"   Total images: {total_images}")
            print(f"   Total text length: {total_text_length}")
            
            # Check a few pages for content
            for i, page in enumerate(pages[:3]):  # Check first 3 pages
                page_num = page.get("page_number", i+1)
                text_len = len(page.get("text", ""))
                tables_count = len(page.get("tables", []))
                images_count = len(page.get("images", []))
                
                print(f"   Page {page_num}: {text_len} chars, {tables_count} tables, {images_count} images")
        
        print()
        print("✅ Verification completed successfully!")
        return True
        
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    # Default file path
    default_file = "outputs/bray_sample_adobe_restructured.json"
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = default_file
    
    # Check if file exists
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        print(f"Available files in outputs/ directory:")
        outputs_dir = Path("outputs")
        if outputs_dir.exists():
            for file in outputs_dir.glob("*.json"):
                print(f"   {file.name}")
        return 1
    
    success = verify_json_structure(file_path)
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
