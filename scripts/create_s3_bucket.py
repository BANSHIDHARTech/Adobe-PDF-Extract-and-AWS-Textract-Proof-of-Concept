"""
Simple script to create an S3 bucket for AWS Textract POC
"""
import boto3
import logging
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_s3_bucket(bucket_name, region='ap-south-1'):
    """Create an S3 bucket in the specified region"""
    try:
        # Create S3 client
        s3_client = boto3.client('s3', region_name=region)
        
        # Create bucket
        logger.info(f"Creating S3 bucket: {bucket_name} in region: {region}")
        
        if region == 'us-east-1':
            # us-east-1 is the default region and doesn't need LocationConstraint
            response = s3_client.create_bucket(Bucket=bucket_name)
        else:
            # Other regions need LocationConstraint
            response = s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        
        logger.info(f"✅ S3 bucket '{bucket_name}' created successfully!")
        logger.info(f"Bucket ARN: arn:aws:s3:::{bucket_name}")
        
        # Set bucket versioning (optional but recommended)
        try:
            s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            logger.info("✅ Bucket versioning enabled")
        except Exception as e:
            logger.warning(f"Could not enable bucket versioning: {e}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            logger.info(f"✅ Bucket '{bucket_name}' already exists!")
            return True
        elif error_code == 'BucketAlreadyOwnedByYou':
            logger.info(f"✅ Bucket '{bucket_name}' already owned by you!")
            return True
        else:
            logger.error(f"❌ Failed to create bucket: {e}")
            return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

def list_existing_buckets():
    """List all existing S3 buckets"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()
        
        if response['Buckets']:
            logger.info("📋 Existing S3 buckets:")
            for bucket in response['Buckets']:
                logger.info(f"   - {bucket['Name']} (created: {bucket['CreationDate']})")
        else:
            logger.info("ℹ️ No S3 buckets found")
            
        return [bucket['Name'] for bucket in response['Buckets']]
        
    except Exception as e:
        logger.error(f"❌ Could not list buckets: {e}")
        return []

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Create S3 bucket for AWS Textract POC')
    parser.add_argument('--bucket', default='my-textract-poc-2025', help='S3 bucket name to create')
    parser.add_argument('--region', default='ap-south-1', help='AWS region for the bucket')
    parser.add_argument('--list', action='store_true', help='List existing buckets instead of creating')
    
    args = parser.parse_args()
    
    if args.list:
        list_existing_buckets()
    else:
        success = create_s3_bucket(args.bucket, args.region)
        if success:
            logger.info(f"\n🚀 Now you can run the Textract script:")
            logger.info(f"python scripts/aws_textract_poc.py --input inputs\\bray_sample.pdf --bucket {args.bucket} --key poc/bray_sample.pdf --out outputs\\textract_extraction.json")
        else:
            logger.error("❌ Failed to create bucket. Please check your AWS credentials and try again.")
