"""
Minimal AWS Textract POC for a single PDF document:
- Uploads file to S3
- Starts async Textract job
- Polls until job completes
- Collects Blocks results and writes JSON to outputs
Requires: pip install boto3 python-dotenv
"""
import os, time, json, argparse, logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
AWS_REGION = os.getenv('AWS_REGION', 'ap-south-1')

def upload_to_s3(s3, local_path, bucket, key):
    """Upload file to S3 bucket"""
    logger.info(f"Uploading {local_path} to s3://{bucket}/{key}")
    try:
        s3.upload_file(local_path, bucket, key)
        logger.info("Upload completed successfully")
    except ClientError as e:
        logger.error(f"Failed to upload file to S3: {e}")
        raise

def start_and_wait(textract, bucket, key, poll_interval=5):
    """Start Textract job and poll until completion"""
    logger.info("Starting Textract document text detection job")
    
    try:
        resp = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
        )
        job_id = resp['JobId']
        logger.info(f"Started Textract job: {job_id}")
        
        all_blocks = []
        next_token = None
        
        while True:
            try:
                if next_token:
                    result = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
                else:
                    result = textract.get_document_text_detection(JobId=job_id)
                    
                status = result.get('JobStatus')
                logger.info(f"Job status: {status}")
                
                if status == 'SUCCEEDED':
                    all_blocks.extend(result.get('Blocks', []))
                    next_token = result.get('NextToken')
                    
                    # Collect remaining pages if NextToken present
                    while next_token:
                        logger.info("Collecting additional pages...")
                        r = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
                        all_blocks.extend(r.get('Blocks', []))
                        next_token = r.get('NextToken')
                    
                    logger.info(f"Job completed successfully. Collected {len(all_blocks)} blocks")
                    break
                    
                elif status == 'FAILED':
                    error_msg = result.get('StatusMessage', 'Unknown error')
                    raise SystemExit(f"Textract job failed: {error_msg}")
                    
                elif status in ['IN_PROGRESS', 'PARTIAL_SUCCESS']:
                    logger.info(f"Job still processing... waiting {poll_interval} seconds")
                    time.sleep(poll_interval)
                else:
                    logger.warning(f"Unexpected job status: {status}")
                    time.sleep(poll_interval)
                    
            except ClientError as e:
                logger.error(f"Error polling Textract job: {e}")
                raise
                
        return all_blocks
        
    except ClientError as e:
        logger.error(f"Failed to start Textract job: {e}")
        raise

def run(input_pdf, bucket, key, out_json, region=AWS_REGION):
    """Main function to run AWS Textract extraction"""
    logger.info(f"Starting AWS Textract extraction for: {input_pdf}")
    
    # Verify input file exists
    if not os.path.exists(input_pdf):
        raise SystemExit(f"Input PDF not found: {input_pdf}")
    
    try:
        # Setup AWS clients
        session = boto3.Session()
        s3 = session.client('s3', region_name=region)
        textract = session.client('textract', region_name=region)
        
        # Create outputs directory
        os.makedirs('outputs', exist_ok=True)
        
        # Upload file to S3
        upload_to_s3(s3, input_pdf, bucket, key)
        
        # Start Textract job and wait for completion
        blocks = start_and_wait(textract, bucket, key)
        
        # Save results
        result_data = {
            'Blocks': blocks,
            'DocumentMetadata': {
                'Pages': len(set(block.get('Page', 1) for block in blocks if 'Page' in block))
            }
        }
        
        with open(out_json, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2)
            
        logger.info(f"Textract extraction completed successfully. Results saved to: {out_json}")
        return 0
        
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        return 1
    except Exception as e:
        logger.error(f"Textract extraction failed: {str(e)}")
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract text and positional data from PDF using AWS Textract')
    parser.add_argument('--input', required=True, help='Input PDF file path')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--key', default='poc/bray_sample.pdf', help='S3 object key')
    parser.add_argument('--out', default='outputs/textract_extraction.json', help='Output JSON file path')
    parser.add_argument('--region', default=AWS_REGION, help='AWS region')
    
    args = parser.parse_args()
    exit_code = run(args.input, args.bucket, args.key, args.out, args.region)
    exit(exit_code)