"""
Minimal Adobe PDF Extract POC.
Requires: pip install pdfservices-sdk python-dotenv
Drop pdfservices-api-credentials.json in repo root or set ADOBE_CREDENTIALS_PATH env var.
"""
import os, zipfile, json, argparse, logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
ADOBE_CREDS = os.getenv('ADOBE_CREDENTIALS_PATH', 'pdfservices-api-credentials.json')
os.makedirs('outputs', exist_ok=True)

def run(input_pdf, creds_path, output_json):
    """Extract text and positional data from PDF using Adobe PDF Extract API"""
    logger.info(f"Starting Adobe PDF extraction for: {input_pdf}")
    
    # Import Adobe PDF Services SDK v4.x
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
        logger.info("✅ Adobe PDF Services SDK v4.x imported successfully")
    except ImportError as e:
        raise SystemExit(f"❌ Missing pdfservices-sdk. Run: pip install pdfservices-sdk\nError: {e}")

    # Verify input files exist
    if not os.path.exists(input_pdf):
        raise SystemExit(f"❌ Input PDF not found: {input_pdf}")
    if not os.path.exists(creds_path):
        raise SystemExit(f"❌ Adobe credentials file not found: {creds_path}")

    try:
        # Setup credentials for SDK v4.x
        logger.info("Setting up Adobe PDF Services credentials")
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        # Handle different credential file formats
        if 'client_credentials' in creds_data:
            # New format
            client_id = creds_data['client_credentials']['client_id']
            client_secret = creds_data['client_credentials']['client_secret']
        elif 'project' in creds_data:
            # Project-based format from Adobe Developer Console
            project = creds_data['project']
            if 'workspace' in project and 'details' in project['workspace']:
                details = project['workspace']['details']
                if 'credentials' in details and isinstance(details['credentials'], list) and len(details['credentials']) > 0:
                    # Take first credential set
                    first_cred = details['credentials'][0]
                    if 'oauth_server_to_server' in first_cred:
                        oauth_creds = first_cred['oauth_server_to_server']
                        client_id = oauth_creds.get('client_id')
                        # client_secrets is an array, take the first one
                        client_secrets = oauth_creds.get('client_secrets', [])
                        client_secret = client_secrets[0] if client_secrets else None
                    elif 'jwt' in first_cred:
                        jwt_creds = first_cred['jwt']
                        client_id = jwt_creds.get('client_id')
                        client_secret = jwt_creds.get('client_secret')
                    else:
                        # Look for client_id/client_secret directly in credential
                        client_id = first_cred.get('client_id')
                        client_secret = first_cred.get('client_secret')
                else:
                    # Fallback: try direct access in project
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
                            # credentials is not a list, try direct access
                            client_id = project['credentials'].get('client_id')
                            client_secret = project['credentials'].get('client_secret')
                    else:
                        # Look for client_id/client_secret directly in project
                        client_id = project.get('client_id')
                        client_secret = project.get('client_secret')
        elif 'client_id' in creds_data:
            # Direct format
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
        
        # Validate that we have valid credentials
        if not client_id or not client_secret:
            raise ValueError(f"Invalid credentials: client_id={client_id}, client_secret={'*' * len(client_secret) if client_secret else 'None'}")
        
        logger.info(f"Using client_id: {client_id[:8]}...")
        logger.info(f"Using client_secret: {'*' * min(8, len(client_secret))}...")
        
        credentials = ServicePrincipalCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
        
        # Create PDF Services instance
        pdf_services = PDFServices(credentials=credentials)
        
        # Create asset from input file
        logger.info("Creating input asset from PDF file")
        with open(input_pdf, 'rb') as file:
            input_stream = file.read()
        
        input_asset = pdf_services.upload(input_stream=input_stream, mime_type=PDFServicesMediaType.PDF)
        
        # Create extract parameters - extract comprehensive data with supported element types
        logger.info("Configuring extraction parameters for comprehensive text and positional data")
        extract_pdf_params = ExtractPDFParams(
            elements_to_extract=[
                ExtractElementType.TEXT,           # All text elements with positions
                ExtractElementType.TABLES          # Table structures
            ]
        )
        
        # Create extract job
        extract_pdf_job = ExtractPDFJob(input_asset=input_asset, extract_pdf_params=extract_pdf_params)
        
        # Submit job and get result
        logger.info("Executing PDF extraction operation")
        location = pdf_services.submit(extract_pdf_job)
        pdf_services_response = pdf_services.get_job_result(location, ExtractPDFResult)
        
        # Get result asset
        result_asset: CloudAsset = pdf_services_response.get_result().get_resource()
        stream_asset: StreamAsset = pdf_services.get_content(result_asset)
        
        # Save result as zip
        result_zip = 'outputs/adobe_extract_result.zip'
        with open(result_zip, "wb") as file:
            file.write(stream_asset.get_input_stream())
        
        logger.info(f"Extraction result saved to: {result_zip}")
        
        # Extract JSON from zip
        with zipfile.ZipFile(result_zip, 'r') as z:
            json_files = [n for n in z.namelist() if n.lower().endswith('.json')]
            if not json_files:
                raise SystemExit("❌ No JSON found in Adobe result zip")
            
            json_name = json_files[0]  # Take first JSON file
            logger.info(f"Extracting JSON file: {json_name}")
            
            with z.open(json_name) as f:
                data = json.load(f)
            
            # Log extraction summary
            if 'elements' in data:
                element_types = {}
                for element in data['elements']:
                    if 'Path' in element:
                        path = element['Path']
                        element_type = path.split('/')[-1] if '/' in path else 'Unknown'
                        element_types[element_type] = element_types.get(element_type, 0) + 1
                
                logger.info("📊 Extraction Summary:")
                for elem_type, count in element_types.items():
                    logger.info(f"   {elem_type}: {count} elements")
                
                if 'extended_metadata' in data and 'page_count' in data['extended_metadata']:
                    logger.info(f"   Total Pages: {data['extended_metadata']['page_count']}")
            
            # Save extracted JSON
            with open(output_json, 'w', encoding='utf-8') as out:
                json.dump(data, out, indent=2)
        
        logger.info(f"✅ Adobe extraction completed successfully. Results saved to: {output_json}")
        logger.info(f"📁 Full results also saved to: {result_zip}")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Adobe extraction failed: {str(e)}")
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract text and positional data from PDF using Adobe PDF Extract API')
    parser.add_argument('--input', required=True, help='Input PDF file path')
    parser.add_argument('--creds', default=ADOBE_CREDS, help='Adobe credentials JSON file path')
    parser.add_argument('--out', default='outputs/adobe_extraction.json', help='Output JSON file path')
    
    args = parser.parse_args()
    exit_code = run(args.input, args.creds, args.out)
    exit(exit_code)
