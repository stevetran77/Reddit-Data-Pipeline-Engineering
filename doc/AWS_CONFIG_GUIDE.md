# AWS Configuration Guide for config.conf

Follow this step-by-step guide to gather all AWS information from the AWS Console UI and fill config.conf.

---

## STEP 1: Get AWS Account ID

1. Go to: https://console.aws.amazon.com/
2. Log in with your AWS credentials
3. **Top right corner** → Click your account name dropdown
4. Select "Account"
5. You'll see your **Account ID** (12 digits) like `123456789012`
6. **Save this value** - you'll need it for IAM role ARNs

---

## STEP 2: Get AWS Access Keys (Create IAM User)

### Create a new IAM User (recommended - don't use root account)

1. Go to: https://console.aws.amazon.com/iam/
2. Left sidebar → **Users**
3. Click **"Create user"** button
4. **User name**: `openaq-pipeline` (or any name you prefer)
5. Click **"Next"**
6. **Attach policies directly:**
   - Search for and check: `AmazonS3FullAccess`
   - Search for and check: `AWSGlueFullAccess`
   - Search for and check: `AmazonRedshiftFullAccess`
7. Click **"Next"** → **"Create user"**

### Generate Access Key

1. Go to: https://console.aws.amazon.com/iam/home?#/users
2. Click on the user you just created (`openaq-pipeline`)
3. Scroll down to **"Access keys"** section
4. Click **"Create access key"**
5. Choose **"Application running outside AWS"** (for local development)
6. Click **"Next"**
7. Add description: `OpenAQ Data Pipeline Local Dev`
8. Click **"Create access key"**
9. **IMPORTANT:** Copy and save these values:
   - **Access key ID** (starts with `AKIA...`)
   - **Secret access key** (long string)
   - ⚠️ You can only see the secret key ONCE. Save it securely!

---

## STEP 3: Get/Create S3 Bucket

1. Go to: https://console.aws.amazon.com/s3/
2. Click **"Create bucket"** button
3. **Bucket name**: `openaq-data-pipeline` (must be globally unique)
   - If taken, add your username/date: `openaq-data-pipeline-yourname`
4. **AWS Region**: ap-southeast-1 (or your preferred region)
5. Keep defaults for other settings
6. Click **"Create bucket"**
7. **Save the bucket name** - you'll use it in config.conf

---

## STEP 4: Create Glue Database

1. Go to: https://console.aws.amazon.com/glue/
2. Left sidebar → **Databases** (under Data Catalog)
3. Click **"Create database"**
4. **Database name**: `openaq_database`
5. Click **"Create database"**
6. **Database created** ✓

---

## STEP 5: Create Glue Crawler

1. Go to: https://console.aws.amazon.com/glue/
2. Left sidebar → **Crawlers** (under Data Catalog)
3. Click **"Create crawler"**
4. **Crawler name**: `openaq_s3_crawler`
5. Click **"Next"**
6. **Data source:**
   - Choose **"S3"**
   - **S3 path**: `s3://your-bucket-name/airquality/` (use your actual bucket name)
7. Click **"Next"**
8. **Create an IAM role:**
   - Choose **"Create an IAM role"**
   - **IAM role name**: `GlueServiceRole`
9. Click **"Next"**
10. **Database:**
    - Choose **"openaq_database"** (you created this in Step 4)
11. Click **"Next"** → **"Create crawler"**
12. **Crawler created** ✓ (Note the role ARN that was created)

---

## STEP 6: Get Glue IAM Role ARN

1. Go to: https://console.aws.amazon.com/iam/
2. Left sidebar → **Roles**
3. Search for: `GlueServiceRole`
4. Click on it
5. Copy the **ARN** (looks like: `arn:aws:iam::123456789012:role/GlueServiceRole`)
6. **Save this ARN** - you'll use it in config.conf

---

## STEP 7: Create Redshift Cluster (if you don't have one)

1. Go to: https://console.aws.amazon.com/redshift/
2. Click **"Create cluster"**
3. **Cluster identifier**: `openaq-warehouse`
4. **Database name**: `openaq_warehouse`
5. **Admin user name**: `admin`
6. **Admin user password**: Create a strong password (save it!)
7. **Cluster configuration:** Keep defaults
8. Click **"Create cluster"**
9. ⏳ **Wait 5-10 minutes** for cluster to be available
10. Once "Available", note the **Endpoint** (looks like: `openaq-warehouse.xxxxx.ap-southeast-1.redshift.amazonaws.com`)

---

## STEP 8: Get Redshift Cluster Details

1. Go to: https://console.aws.amazon.com/redshift/
2. Click on your cluster: `openaq-warehouse`
3. Copy these values:
   - **Endpoint**: Remove the `:5439` part if present (e.g., `openaq-warehouse.xxxxx.ap-southeast-1.redshift.amazonaws.com`)
   - **Port**: `5439` (default)
   - **Database**: `openaq_warehouse`
   - **Master username**: `admin`
   - **Master user password**: What you created in Step 7

---

## STEP 9: Create Redshift S3 Role

1. Go to: https://console.aws.amazon.com/iam/
2. Left sidebar → **Roles**
3. Click **"Create role"**
4. **Trusted entity type**: `AWS service`
5. **Use case**: Search for `Redshift` → Select `Redshift: Customizable`
6. Click **"Next"**
7. Search for and check: `AmazonS3FullAccess`
8. Click **"Next"**
9. **Role name**: `RedshiftS3Role`
10. Click **"Create role"**
11. Copy the **ARN** (looks like: `arn:aws:iam::123456789012:role/RedshiftS3Role`)
12. **Save this ARN** - you'll use it in config.conf

---

---

## ⭐ ALTERNATIVE: SETUP ATHENA (Cost-Effective Free Tier Option)

**Skip steps 7-10 (Redshift) and do this instead:**

### ATHENA STEP 1: Create S3 Bucket for Query Results

1. Go to: https://console.aws.amazon.com/s3/
2. Click **"Create bucket"**
3. **Bucket name**: `openaq-athena-results` (or add your name if taken)
4. **Region**: `ap-southeast-1` (same as your main bucket)
5. Click **"Create bucket"**

### ATHENA STEP 2: Enable Athena

1. Go to: https://console.aws.amazon.com/athena/
2. If first time, click **"Getting started"** and accept terms
3. Click **"Query editor"** tab

### ATHENA STEP 3: Configure Query Results Location

1. In Query editor, click **"Settings"** button (⚙️ icon)
2. Under "Query result location":
   - Click **"Browse S3"**
   - Select: `openaq-athena-results`
   - Click **"Save"**

### ATHENA STEP 4: Test Athena

Run this query in Query Editor to verify Glue integration:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'openaq_database'
LIMIT 10;
```

**Expected**: You should see tables from Glue Crawler ✅

---

## Summary - Config Values

### For Athena (Recommended - Free Tier):

| Config Key | Value |
|-----------|-------|
| `aws_access_key_id` | `AKIA...` (from Step 2) |
| `aws_secret_access_key` | Long string (from Step 2) |
| `aws_region` | `ap-southeast-1` |
| `aws_bucket_name` | `openaq-data-pipeline` |
| `glue_database_name` | `openaq_database` |
| `glue_crawler_name` | `openaq_s3_crawler` |
| `glue_iam_role` | `arn:aws:iam::387158739004:role/service-role/AWSGlueServiceRole-steve_tran` |
| `athena_database` | `openaq_database` |
| `athena_output_location` | `s3://openaq-athena-results` |

**✅ Already updated in your config.conf!**

### For Redshift (Optional - Use if you want):

| Config Key | Value | Source |
|-----------|-------|--------|
| `redshift_host` | `openaq-warehouse.xxxxx.ap-southeast-1.redshift.amazonaws.com` | Redshift Console |
| `redshift_port` | `5439` | Default |
| `redshift_database` | `openaq_warehouse` | Your choice |
| `redshift_username` | `admin` | Your choice |
| `redshift_password` | Your password | Your choice |
| `redshift_iam_role` | `arn:aws:iam::123456789012:role/RedshiftS3Role` | IAM Console |

---

## Next Steps

1. **Athena option (Recommended)**: Your config is already updated!
   - Run: `python test_athena_connection.py`
   - Then proceed to connect Looker

2. **Redshift option**: Complete steps 7-10 if you choose Redshift instead
   - Then run: `python test_connection.py`

