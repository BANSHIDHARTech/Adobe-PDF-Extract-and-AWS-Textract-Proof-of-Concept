"""
Fixed AWS Textract POC for a single PDF document:
- Handles permission issues better
- Better error handling for S3 and Textract
- Region-aware S3 operations
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

def upload_to_s3(s3, local_path, bucket, key, region):
    """Upload file to S3 bucket with proper region handling"""
    logger.info(f"Uploading {local_path} to s3://{bucket}/{key} in region {region}")
    try:
        # Use the same region for S3 operations
        s3_client = boto3.client('s3', region_name=region)
        s3_client.upload_file(local_path, bucket, key)
        logger.info("✅ Upload completed successfully")
        
        # Verify the upload by checking if object exists
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            logger.info("✅ Object verified in S3")
            return True
        except ClientError as e:
            logger.error(f"❌ Object verification failed: {e}")
            return False
            
    except ClientError as e:
        logger.error(f"❌ Failed to upload file to S3: {e}")
        raise

def start_and_wait(textract, bucket, key, region, poll_interval=5):
    """Start Textract job and poll until completion"""
    logger.info(f"Starting Textract document text detection job in region {region}")
    
    try:
        # Make sure we're using the correct region for Textract
        textract_client = boto3.client('textract', region_name=region)
        
        resp = textract_client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
        )
        job_id = resp['JobId']
        logger.info(f"✅ Started Textract job: {job_id}")
        
        all_blocks = []
        next_token = None
        
        while True:
            try:
                if next_token:
                    result = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
                else:
                    result = textract_client.get_document_text_detection(JobId=job_id)
                    
                status = result.get('JobStatus')
                logger.info(f"Job status: {status}")
                
                if status == 'SUCCEEDED':
                    all_blocks.extend(result.get('Blocks', []))
                    next_token = result.get('NextToken')
                    
                    # Collect remaining pages if NextToken present
                    while next_token:
                        logger.info("Collecting additional pages...")
                        r = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
                        all_blocks.extend(r.get('Blocks', []))
                        next_token = r.get('NextToken')
                    
                    logger.info(f"✅ Job completed successfully. Collected {len(all_blocks)} blocks")
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

def check_bucket_permissions(s3, bucket, region):
    """Check if we have proper permissions on the bucket"""
    logger.info(f"🔍 Checking permissions on bucket: {bucket}")
    
    try:
        # Try to list objects (read permission)
        s3_client = boto3.client('s3', region_name=region)
        response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        logger.info("✅ Read permission confirmed")
        
        # Try to get bucket location
        try:
            location = s3_client.get_bucket_location(Bucket=bucket)
            bucket_region = location.get('LocationConstraint') or 'us-east-1'
            logger.info(f"✅ Bucket region: {bucket_region}")
            
            if bucket_region != region:
                logger.warning(f"⚠️ Warning: Bucket is in {bucket_region} but you're using {region}")
                logger.info(f"Consider using --region {bucket_region}")
                
        except ClientError as e:
            logger.warning(f"Could not determine bucket region: {e}")
            
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            logger.error(f"❌ Access denied to bucket {bucket}")
            logger.error("Your AWS user doesn't have permission to read from this bucket")
            return False
        elif error_code == 'NoSuchBucket':
            logger.error(f"❌ Bucket {bucket} does not exist")
            return False
        else:
            logger.error(f"❌ Permission check failed: {e}")
            return False

def run(input_pdf, bucket, key, out_json, region=AWS_REGION):
    """Main function to run AWS Textract extraction"""
    logger.info(f"🚀 Starting AWS Textract extraction for: {input_pdf}")
    logger.info(f"📦 Target: s3://{bucket}/{key}")
    logger.info(f"🌍 Region: {region}")
    
    # Verify input file exists
    if not os.path.exists(input_pdf):
        raise SystemExit(f"❌ Input PDF not found: {input_pdf}")
    
    try:
        # Setup AWS clients
        session = boto3.Session()
        s3 = session.client('s3', region_name=region)
        textract = session.client('textract', region_name=region)
        
        # Check bucket permissions first
        if not check_bucket_permissions(s3, bucket, region):
            logger.error("❌ Cannot proceed due to permission issues")
            logger.info("💡 Solutions:")
            logger.info("   1. Ask your AWS admin to grant S3 read/write permissions")
            logger.info("   2. Use a different bucket that you have access to")
            logger.info("   3. Check if the bucket exists and is in the correct region")
            return 1
        
        # Create outputs directory
        os.makedirs('outputs', exist_ok=True)
        
        # Upload file to S3
        if not upload_to_s3(s3, input_pdf, bucket, key, region):
            logger.error("❌ S3 upload failed")
            return 1
        
        # Start Textract job and wait for completion
        blocks = start_and_wait(textract, bucket, key, region)
        
        # Save results
        result_data = {
            'Blocks': blocks,
            'DocumentMetadata': {
                'Pages': len(set(block.get('Page', 1) for block in blocks if 'Page' in block))
            },
            'ExtractionInfo': {
                'Bucket': bucket,
                'Key': key,
                'Region': region,
                'TotalBlocks': len(blocks)
            }
        }
        
        with open(out_json, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2)
            
        logger.info(f"🎉 Textract extraction completed successfully!")
        logger.info(f"📁 Results saved to: {out_json}")
        logger.info(f"📊 Total blocks extracted: {len(blocks)}")
        return 0
        
    except NoCredentialsError:
        logger.error("❌ AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        return 1
    except Exception as e:
        logger.error(f"❌ Textract extraction failed: {str(e)}")
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
