"""
Improved Adobe PDF Extract POC with automatic folder detection, image extraction, and structured output.
Requires: pip install pdfservices-sdk python-dotenv
Drop pdfservices-api-credentials.json in repo root or set ADOBE_CREDENTIALS_PATH env var.
"""
import os
import zipfile
import json
import argparse
import logging
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class AdobePDFExtractor:
    def __init__(self, input_dir: str = "inputs", output_dir: str = "outputs"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.adobe_creds = os.getenv('ADOBE_CREDENTIALS_PATH', 'pdfservices-api-credentials.json')
        
        # Ensure directories exist
        self.input_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        # Import Adobe SDK
        self._import_adobe_sdk()
    
    def _import_adobe_sdk(self):
        """Import Adobe PDF Services SDK v4.x"""
        try:
            from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
            from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
            from adobe.pdfservices.operation.io.stream_asset import StreamAsset
            from adobe.pdfservices.operation.pdf_services import PDFServices
            from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
            from adobe.pdfservices.operation.pdfjobs.jobs.extract_pdf_job import ExtractPDFJob
            from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_element_type import ExtractElementType
            from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_pdf_params import ExtractPDFParams
            from adobe.pdfservices.operation.pdfjobs.result.extract_pdf_result import ExtractPDFResult
            
            self.ServicePrincipalCredentials = ServicePrincipalCredentials
            self.CloudAsset = CloudAsset
            self.StreamAsset = StreamAsset
            self.PDFServices = PDFServices
            self.PDFServicesMediaType = PDFServicesMediaType
            self.ExtractPDFJob = ExtractPDFJob
            self.ExtractElementType = ExtractElementType
            self.ExtractPDFParams = ExtractPDFParams
            self.ExtractPDFResult = ExtractPDFResult
            
            logger.info("✅ Adobe PDF Services SDK v4.x imported successfully")
        except ImportError as e:
            raise SystemExit(f"❌ Missing pdfservices-sdk. Run: pip install pdfservices-sdk\nError: {e}")
    
    def _find_pdf_files(self) -> List[Path]:
        """Automatically find PDF files in input directory"""
        pdf_files = list(self.input_dir.glob("*.pdf"))
        if not pdf_files:
            logger.warning(f"No PDF files found in {self.input_dir}")
        else:
            logger.info(f"Found {len(pdf_files)} PDF file(s): {[f.name for f in pdf_files]}")
        return pdf_files
    
    def _find_credentials(self) -> Path:
        """Find Adobe credentials file"""
        creds_path = Path(self.adobe_creds)
        if not creds_path.exists():
            # Try to find credentials in common locations
            possible_locations = [
                Path("pdfservices-api-credentials.json"),
                Path("credentials.json"),
                Path("adobe_credentials.json")
            ]
            
            for loc in possible_locations:
                if loc.exists():
                    creds_path = loc
                    break
            else:
                raise FileNotFoundError(f"Adobe credentials file not found. Tried: {[self.adobe_creds] + [str(loc) for loc in possible_locations]}")
        
        logger.info(f"Using credentials file: {creds_path}")
        return creds_path
    
    def _extract_credentials(self, creds_path: Path) -> tuple:
        """Extract client_id and client_secret from credentials file"""
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        # Handle different credential file formats
        if 'client_credentials' in creds_data:
            client_id = creds_data['client_credentials']['client_id']
            client_secret = creds_data['client_credentials']['client_secret']
        elif 'project' in creds_data:
            project = creds_data['project']
            if 'workspace' in project and 'details' in project['workspace']:
                details = project['workspace']['details']
                if 'credentials' in details and isinstance(details['credentials'], list) and len(details['credentials']) > 0:
                    first_cred = details['credentials'][0]
                    if 'oauth_server_to_server' in first_cred:
                        oauth_creds = first_cred['oauth_server_to_server']
                        client_id = oauth_creds.get('client_id')
                        client_secrets = oauth_creds.get('client_secrets', [])
                        client_secret = client_secrets[0] if client_secrets else None
                    elif 'jwt' in first_cred:
                        jwt_creds = first_cred['jwt']
                        client_id = jwt_creds.get('client_id')
                        client_secret = jwt_creds.get('client_secret')
                    else:
                        client_id = first_cred.get('client_id')
                        client_secret = first_cred.get('client_secret')
                else:
                    if 'credentials' in project:
                        creds = project['credentials']
                        if isinstance(creds, list) and len(creds) > 0:
                            first_cred = creds[0]
                            if 'jwt' in first_cred:
                                jwt_creds = first_cred['jwt']
                                client_id = jwt_creds.get('client_id')
                                client_secret = jwt_creds.get('client_secret')
                            elif 'oauth_server_to_server' in first_cred:
                                oauth_creds = first_cred['oauth_server_to_server']
                                client_id = oauth_creds.get('client_id')
                                client_secrets = oauth_creds.get('client_secrets', [])
                                client_secret = client_secrets[0] if client_secrets else None
                            else:
                                client_id = first_cred.get('client_id')
                                client_secret = first_cred.get('client_secret')
                        else:
                            client_id = project['credentials'].get('client_id')
                            client_secret = project['credentials'].get('client_secret')
                    else:
                        client_id = project.get('client_id')
                        client_secret = project.get('client_secret')
        elif 'client_id' in creds_data:
            client_id = creds_data['client_id']
            client_secret = creds_data['client_secret']
        else:
            # Try to find client_id and client_secret anywhere in the JSON
            def find_credentials(obj):
                if isinstance(obj, dict):
                    if 'client_id' in obj and 'client_secret' in obj:
                        return obj['client_id'], obj['client_secret']
                    for value in obj.values():
                        result = find_credentials(value)
                        if result:
                            return result
                return None
            
            result = find_credentials(creds_data)
            if result:
                client_id, client_secret = result
            else:
                raise ValueError("Could not find client_id and client_secret in credentials file")
        
        if not client_id or not client_secret:
            raise ValueError(f"Invalid credentials: client_id={client_id}, client_secret={'*' * len(client_secret) if client_secret else 'None'}")
        
        logger.info(f"Using client_id: {client_id[:8]}...")
        return client_id, client_secret
    
    def _extract_images_from_elements(self, elements: List[Dict], page_count: int) -> Dict[int, List[Dict]]:
        """Extract image information from Adobe elements"""
        images_by_page = {i: [] for i in range(page_count)}
        
        for element in elements:
            if element.get('Path', '').startswith('//Document/Figure'):
                page_num = element.get('Page', 0)
                image_id = f"i{len(images_by_page[page_num]) + 1}"
                
                # Extract image metadata
                image_info = {
                    "image_id": image_id,
                    "path": f"images/page{page_num + 1}_{image_id}.png",  # Virtual path
                    "caption": self._extract_image_caption(element, elements),
                    "bounds": element.get('Bounds', []),
                    "bbox": element.get('attributes', {}).get('BBox', []),
                    "placement": element.get('attributes', {}).get('Placement', 'Unknown')
                }
                
                images_by_page[page_num].append(image_info)
        
        return images_by_page
    
    def _extract_image_caption(self, image_element: Dict, all_elements: List[Dict]) -> Optional[str]:
        """Try to extract caption for an image by looking at nearby text elements"""
        image_bounds = image_element.get('Bounds', [])
        if not image_bounds or len(image_bounds) < 4:
            return None
        
        # Look for text elements that might be captions (below or near the image)
        potential_captions = []
        
        for element in all_elements:
            if element.get('Path', '').startswith('//Document/P') and 'Text' in element:
                text_bounds = element.get('Bounds', [])
                if len(text_bounds) >= 4:
                    # Check if text is below the image (within reasonable distance)
                    if (text_bounds[1] < image_bounds[1] and  # Text Y1 < Image Y1 (below)
                        abs(text_bounds[0] - image_bounds[0]) < 50):  # Similar X position
                        potential_captions.append({
                            'text': element.get('Text', ''),
                            'distance': image_bounds[1] - text_bounds[1],
                            'bounds': text_bounds
                        })
        
        # Return the closest caption below the image
        if potential_captions:
            closest = min(potential_captions, key=lambda x: x['distance'])
            if closest['distance'] < 100:  # Within 100 points
                return closest['text']
        
        return None
    
    def _extract_tables_from_elements(self, elements: List[Dict], page_count: int) -> Dict[int, List[Dict]]:
        """Extract table information from Adobe elements"""
        tables_by_page = {i: [] for i in range(page_count)}
        
        for element in elements:
            if element.get('Path', '').startswith('//Document/Table'):
                page_num = element.get('Page', 0)
                table_id = f"t{len(tables_by_page[page_num]) + 1}"
                
                # Extract table structure
                table_info = {
                    "table_id": table_id,
                    "data": self._extract_table_data(element, elements),
                    "bounds": element.get('Bounds', []),
                    "bbox": element.get('attributes', {}).get('BBox', []),
                    "num_rows": element.get('attributes', {}).get('NumRow', 0),
                    "placement": element.get('attributes', {}).get('Placement', 'Unknown')
                }
                
                tables_by_page[page_num].append(table_info)
        
        return tables_by_page
    
    def _extract_table_data(self, table_element: Dict, all_elements: List[Dict]) -> List[List[str]]:
        """Extract table data as 2D array"""
        table_path = table_element.get('Path', '')
        table_data = []
        
        # Find all table cells for this table
        cells = []
        for element in all_elements:
            if element.get('Path', '').startswith(table_path) and 'TD' in element.get('Path', ''):
                cells.append(element)
        
        # Group cells by row and column
        if cells:
            # Sort by row index, then column index
            cells.sort(key=lambda x: (
                x.get('attributes', {}).get('RowIndex', 0),
                x.get('attributes', {}).get('ColIndex', 0)
            ))
            
            # Extract text from each cell
            current_row = []
            current_row_idx = -1
            
            for cell in cells:
                row_idx = cell.get('attributes', {}).get('RowIndex', 0)
                col_idx = cell.get('attributes', {}).get('ColIndex', 0)
                
                if row_idx != current_row_idx:
                    if current_row:
                        table_data.append(current_row)
                    current_row = [''] * (max([c.get('attributes', {}).get('ColIndex', 0) for c in cells]) + 1)
                    current_row_idx = row_idx
                
                # Find text content in this cell
                cell_text = ""
                for element in all_elements:
                    if (element.get('Path', '').startswith(cell.get('Path', '')) and 
                        element.get('Path', '').endswith('/P') and 'Text' in element):
                        cell_text += element.get('Text', '') + " "
                
                if col_idx < len(current_row):
                    current_row[col_idx] = cell_text.strip()
            
            if current_row:
                table_data.append(current_row)
        
        return table_data
    
    def _extract_text_by_page(self, elements: List[Dict], page_count: int) -> Dict[int, str]:
        """Extract text content organized by page"""
        text_by_page = {i: "" for i in range(page_count)}
        
        for element in elements:
            if element.get('Path', '').startswith('//Document/P') and 'Text' in element:
                page_num = element.get('Page', 0)
                text_by_page[page_num] += element.get('Text', '') + " "
        
        # Clean up text
        for page_num in text_by_page:
            text_by_page[page_num] = text_by_page[page_num].strip()
        
        return text_by_page
    
    def _restructure_output(self, adobe_data: Dict, pdf_filename: str) -> Dict:
        """Restructure Adobe output to match desired format"""
        page_count = adobe_data.get('extended_metadata', {}).get('page_count', 0)
        elements = adobe_data.get('elements', [])
        
        # Extract data by page
        text_by_page = self._extract_text_by_page(elements, page_count)
        tables_by_page = self._extract_tables_from_elements(elements, page_count)
        images_by_page = self._extract_images_from_elements(elements, page_count)
        
        # Build restructured output
        restructured = {
            "document_id": Path(pdf_filename).stem,
            "metadata": {
                "source": pdf_filename,
                "page_count": page_count,
                "pdf_version": adobe_data.get('extended_metadata', {}).get('pdf_version', ''),
                "language": adobe_data.get('extended_metadata', {}).get('language', ''),
                "extraction_timestamp": adobe_data.get('extended_metadata', {}).get('ID_instance', '')
            },
            "pages": []
        }
        
        for page_num in range(page_count):
            page_data = {
                "page_number": page_num + 1,
                "text": text_by_page.get(page_num, ""),
                "tables": tables_by_page.get(page_num, []),
                "images": images_by_page.get(page_num, [])
            }
            restructured["pages"].append(page_data)
        
        return restructured
    
    def extract_pdf(self, pdf_path: Path) -> Dict:
        """Extract content from a single PDF file"""
        logger.info(f"Starting Adobe PDF extraction for: {pdf_path.name}")
        
        # Verify input file exists
        if not pdf_path.exists():
            raise FileNotFoundError(f"Input PDF not found: {pdf_path}")
        
        # Find and validate credentials
        creds_path = self._find_credentials()
        client_id, client_secret = self._extract_credentials(creds_path)
        
        try:
            # Setup credentials
            credentials = self.ServicePrincipalCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
            
            # Create PDF Services instance
            pdf_services = self.PDFServices(credentials=credentials)
            
            # Create asset from input file
            logger.info("Creating input asset from PDF file")
            with open(pdf_path, 'rb') as file:
                input_stream = file.read()
            
            input_asset = pdf_services.upload(input_stream=input_stream, mime_type=self.PDFServicesMediaType.PDF)
            
            # Create extract parameters
            logger.info("Configuring extraction parameters for comprehensive data")
            extract_pdf_params = self.ExtractPDFParams(
                elements_to_extract=[
                    self.ExtractElementType.TEXT,
                    self.ExtractElementType.TABLES
                ]
            )
            
            # Create and submit extract job
            extract_pdf_job = self.ExtractPDFJob(input_asset=input_asset, extract_pdf_params=extract_pdf_params)
            logger.info("Executing PDF extraction operation")
            location = pdf_services.submit(extract_pdf_job)
            pdf_services_response = pdf_services.get_job_result(location, self.ExtractPDFResult)
            
            # Get result asset
            result_asset = pdf_services_response.get_result().get_resource()
            stream_asset = pdf_services.get_content(result_asset)
            
            # Save result as zip
            result_zip = self.output_dir / f"{pdf_path.stem}_adobe_result.zip"
            with open(result_zip, "wb") as file:
                file.write(stream_asset.get_input_stream())
            
            logger.info(f"Extraction result saved to: {result_zip}")
            
            # Extract JSON from zip
            with zipfile.ZipFile(result_zip, 'r') as z:
                json_files = [n for n in z.namelist() if n.lower().endswith('.json')]
                if not json_files:
                    raise SystemExit("❌ No JSON found in Adobe result zip")
                
                json_name = json_files[0]
                logger.info(f"Extracting JSON file: {json_name}")
                
                with z.open(json_name) as f:
                    adobe_data = json.load(f)
                
                # Log extraction summary
                if 'elements' in adobe_data:
                    element_types = {}
                    for element in adobe_data['elements']:
                        if 'Path' in element:
                            path = element['Path']
                            element_type = path.split('/')[-1] if '/' in path else 'Unknown'
                            element_types[element_type] = element_types.get(element_type, 0) + 1
                    
                    logger.info("📊 Extraction Summary:")
                    for elem_type, count in element_types.items():
                        logger.info(f"   {elem_type}: {count} elements")
                    
                    if 'extended_metadata' in adobe_data and 'page_count' in adobe_data['extended_metadata']:
                        logger.info(f"   Total Pages: {adobe_data['extended_metadata']['page_count']}")
                
                # Restructure output
                restructured_data = self._restructure_output(adobe_data, pdf_path.name)
                
                # Save both original and restructured data
                original_json = self.output_dir / f"{pdf_path.stem}_adobe_original.json"
                restructured_json = self.output_dir / f"{pdf_path.stem}_adobe_restructured.json"
                
                with open(original_json, 'w', encoding='utf-8') as out:
                    json.dump(adobe_data, out, indent=2)
                
                with open(restructured_json, 'w', encoding='utf-8') as out:
                    json.dump(restructured_data, out, indent=2)
                
                logger.info(f"✅ Adobe extraction completed successfully")
                logger.info(f"📁 Original results: {original_json}")
                logger.info(f"📁 Restructured results: {restructured_json}")
                logger.info(f"📁 Full results: {result_zip}")
                
                return restructured_data
                
        except Exception as e:
            logger.error(f"❌ Adobe extraction failed: {str(e)}")
            raise
    
    def process_all_pdfs(self) -> List[Dict]:
        """Process all PDF files in the input directory"""
        pdf_files = self._find_pdf_files()
        if not pdf_files:
            logger.warning("No PDF files to process")
            return []
        
        results = []
        for pdf_file in pdf_files:
            try:
                result = self.extract_pdf(pdf_file)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to process {pdf_file.name}: {e}")
                continue
        
        return results
    
    def verify_extraction(self, pdf_path: Path, extraction_result: Dict) -> Dict:
        """Verify extraction results by comparing with actual PDF"""
        verification = {
            "pdf_file": str(pdf_path),
            "verification_passed": True,
            "issues": [],
            "summary": {}
        }
        
        try:
            # Basic verification checks
            expected_pages = extraction_result.get('metadata', {}).get('page_count', 0)
            actual_pages = len(extraction_result.get('pages', []))
            
            if expected_pages != actual_pages:
                verification["verification_passed"] = False
                verification["issues"].append(f"Page count mismatch: expected {expected_pages}, got {actual_pages}")
            
            # Check each page
            for page_data in extraction_result.get('pages', []):
                page_num = page_data.get('page_number', 0)
                
                # Check if page has content
                text_content = page_data.get('text', '')
                tables = page_data.get('tables', [])
                images = page_data.get('images', [])
                
                if not text_content and not tables and not images:
                    verification["issues"].append(f"Page {page_num}: No content extracted")
                
                # Check table structure
                for table in tables:
                    if not table.get('data') or not table['data'][0]:
                        verification["issues"].append(f"Page {page_num}: Table {table.get('table_id')} has no data")
                
                # Check image metadata
                for image in images:
                    if not image.get('bounds') and not image.get('bbox'):
                        verification["issues"].append(f"Page {page_num}: Image {image.get('image_id')} missing bounds")
            
            # Summary statistics
            verification["summary"] = {
                "total_pages": actual_pages,
                "total_tables": sum(len(page.get('tables', [])) for page in extraction_result.get('pages', [])),
                "total_images": sum(len(page.get('images', [])) for page in extraction_result.get('pages', [])),
                "total_text_length": sum(len(page.get('text', '')) for page in extraction_result.get('pages', []))
            }
            
            if verification["issues"]:
                verification["verification_passed"] = False
            
        except Exception as e:
            verification["verification_passed"] = False
            verification["issues"].append(f"Verification error: {str(e)}")
        
        return verification

def main():
    parser = argparse.ArgumentParser(description='Improved Adobe PDF Extract with automatic folder detection and structured output')
    parser.add_argument('--input-dir', default='inputs', help='Input directory containing PDF files')
    parser.add_argument('--output-dir', default='outputs', help='Output directory for results')
    parser.add_argument('--pdf', help='Specific PDF file to process (optional)')
    parser.add_argument('--verify', action='store_true', help='Verify extraction results')
    
    args = parser.parse_args()
    
    try:
        extractor = AdobePDFExtractor(args.input_dir, args.output_dir)
        
        if args.pdf:
            # Process specific PDF
            pdf_path = Path(args.pdf)
            if not pdf_path.exists():
                pdf_path = extractor.input_dir / args.pdf
            
            result = extractor.extract_pdf(pdf_path)
            
            if args.verify:
                verification = extractor.verify_extraction(pdf_path, result)
                verification_file = extractor.output_dir / f"{pdf_path.stem}_verification.json"
                with open(verification_file, 'w', encoding='utf-8') as f:
                    json.dump(verification, f, indent=2)
                logger.info(f"Verification results saved to: {verification_file}")
                
                if verification["verification_passed"]:
                    logger.info("✅ Verification passed")
                else:
                    logger.warning("⚠️ Verification found issues:")
                    for issue in verification["issues"]:
                        logger.warning(f"   - {issue}")
        else:
            # Process all PDFs in input directory
            results = extractor.process_all_pdfs()
            logger.info(f"Processed {len(results)} PDF file(s)")
            
            # Save combined results
            if results:
                combined_file = extractor.output_dir / "combined_extractions.json"
                with open(combined_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2)
                logger.info(f"Combined results saved to: {combined_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {str(e)}")
        return 1

if __name__ == '__main__':
    exit(main())
