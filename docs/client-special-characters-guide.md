# Working with Special Characters in Object Names

## Overview

This guide explains how to properly handle special characters (spaces, plus signs, etc.) in S3 object names when using RustFS.

## Quick Reference

| Character | What You Type | How It's Stored | How to Access It |
|-----------|---------------|-----------------|------------------|
| Space | `my file.txt` | `my file.txt` | Use proper S3 client/SDK |
| Plus | `test+file.txt` | `test+file.txt` | Use proper S3 client/SDK |
| Percent | `test%file.txt` | `test%file.txt` | Use proper S3 client/SDK |

**Key Point**: Use a proper S3 SDK or client. They handle URL encoding automatically!

## Recommended Approach: Use S3 SDKs

The easiest and most reliable way to work with object names containing special characters is to use an official S3 SDK. These handle all encoding automatically.

### AWS CLI

```bash
# Works correctly - AWS CLI handles encoding
aws --endpoint-url=http://localhost:9000 s3 cp file.txt "s3://mybucket/path with spaces/file.txt"
aws --endpoint-url=http://localhost:9000 s3 ls "s3://mybucket/path with spaces/"

# Works with plus signs
aws --endpoint-url=http://localhost:9000 s3 cp data.json "s3://mybucket/ES+net/data.json"
```

### MinIO Client (mc)

```bash
# Configure RustFS endpoint
mc alias set myrustfs http://localhost:9000 ACCESS_KEY SECRET_KEY

# Upload with spaces in path
mc cp README.md "myrustfs/mybucket/a f+/b/c/3/README.md"

# List contents
mc ls "myrustfs/mybucket/a f+/"
mc ls "myrustfs/mybucket/a f+/b/c/3/"

# Works with plus signs
mc cp file.txt "myrustfs/mybucket/ES+net/file.txt"
```

### Python (boto3)

```python
import boto3

# Configure client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='ACCESS_KEY',
    aws_secret_access_key='SECRET_KEY'
)

# Upload with spaces - boto3 handles encoding automatically
s3.put_object(
    Bucket='mybucket',
    Key='path with spaces/file.txt',
    Body=b'file content'
)

# List objects - boto3 encodes prefix automatically
response = s3.list_objects_v2(
    Bucket='mybucket',
    Prefix='path with spaces/'
)

for obj in response.get('Contents', []):
    print(obj['Key'])  # Will print: "path with spaces/file.txt"

# Works with plus signs
s3.put_object(
    Bucket='mybucket',
    Key='ES+net/LHC+Data+Challenge/file.json',
    Body=b'data'
)
```

### Go (AWS SDK)

```go
package main

import (
    "bytes"
    "fmt"
    
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

func main() {
    // Configure session
    sess := session.Must(session.NewSession(&aws.Config{
        Endpoint:         aws.String("http://localhost:9000"),
        Region:           aws.String("us-east-1"),
        Credentials:      credentials.NewStaticCredentials("ACCESS_KEY", "SECRET_KEY", ""),
        S3ForcePathStyle: aws.Bool(true),
    }))
    
    svc := s3.New(sess)
    
    // Upload with spaces - SDK handles encoding
    _, err := svc.PutObject(&s3.PutObjectInput{
        Bucket: aws.String("mybucket"),
        Key:    aws.String("path with spaces/file.txt"),
        Body:   bytes.NewReader([]byte("content")),
    })
    
    if err != nil {
        panic(err)
    }
    
    // List objects - SDK handles encoding
    result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
        Bucket: aws.String("mybucket"),
        Prefix: aws.String("path with spaces/"),
    })
    
    if err != nil {
        panic(err)
    }
    
    for _, obj := range result.Contents {
        fmt.Println(*obj.Key)
    }
}
```

### Node.js (AWS SDK v3)

```javascript
const { S3Client, PutObjectCommand, ListObjectsV2Command } = require("@aws-sdk/client-s3");

// Configure client
const client = new S3Client({
  endpoint: "http://localhost:9000",
  region: "us-east-1",
  credentials: {
    accessKeyId: "ACCESS_KEY",
    secretAccessKey: "SECRET_KEY",
  },
  forcePathStyle: true,
});

// Upload with spaces - SDK handles encoding
async function upload() {
  const command = new PutObjectCommand({
    Bucket: "mybucket",
    Key: "path with spaces/file.txt",
    Body: "file content",
  });
  
  await client.send(command);
}

// List objects - SDK handles encoding
async function list() {
  const command = new ListObjectsV2Command({
    Bucket: "mybucket",
    Prefix: "path with spaces/",
  });
  
  const response = await client.send(command);
  
  for (const obj of response.Contents || []) {
    console.log(obj.Key);
  }
}
```

## Advanced: Manual HTTP Requests

**⚠️ Not Recommended**: Only use if you can't use an S3 SDK.

If you must make raw HTTP requests, you need to manually URL-encode the object key in the path:

### URL Encoding Rules

| Character | Encoding | Example |
|-----------|----------|---------|
| Space | `%20` | `my file.txt` → `my%20file.txt` |
| Plus | `%2B` | `test+file.txt` → `test%2Bfile.txt` |
| Percent | `%25` | `test%file.txt` → `test%25file.txt` |
| Slash (in name) | `%2F` | `test/file.txt` → `test%2Ffile.txt` |

**Important**: In URL **paths** (not query parameters):
- `%20` = space
- `+` = literal plus sign (NOT space!)
- To represent a plus sign, use `%2B`

### Example: Manual curl Request

```bash
# Upload object with spaces
curl -X PUT "http://localhost:9000/mybucket/path%20with%20spaces/file.txt" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     -d "file content"

# Upload object with plus signs
curl -X PUT "http://localhost:9000/mybucket/ES%2Bnet/file.txt" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     -d "data"

# List objects (prefix in query parameter)
curl "http://localhost:9000/mybucket?prefix=path%20with%20spaces/"

# Note: You'll also need to compute AWS Signature V4
# This is complex - use an SDK instead!
```

## Troubleshooting

### Issue: "UI can navigate to folder but can't list contents"

**Symptom**: 
- You uploaded: `mc cp file.txt "myrustfs/bucket/a f+/b/c/file.txt"`
- You can see folder `"a f+"` in the UI
- But clicking on it shows "No Objects"

**Root Cause**: The UI may not be properly URL-encoding the prefix when making the LIST request.

**Solution**:
1. **Use CLI instead**: `mc ls "myrustfs/bucket/a f+/b/c/"` works correctly
2. **Check UI console**: Open browser DevTools, look at Network tab, check if the request is properly encoded
3. **Report UI bug**: If using RustFS web console, this is a UI bug to report

**Workaround**:
Use the CLI for operations with special characters until UI is fixed.

### Issue: "400 Bad Request: Invalid Argument"

**Symptom**:
```
Error: api error InvalidArgument: Invalid argument
```

**Possible Causes**:

1. **Client not encoding plus signs**
   - Problem: Client sends `/bucket/ES+net/file.txt` 
   - Solution: Client should send `/bucket/ES%2Bnet/file.txt`
   - Fix: Use a proper S3 SDK

2. **Control characters in key**
   - Problem: Key contains null bytes, newlines, etc.
   - Solution: Remove invalid characters from key name

3. **Double-encoding**
   - Problem: Client encodes twice: `%20` → `%2520`
   - Solution: Only encode once, or use SDK

**Debugging**:
Enable debug logging on RustFS:
```bash
RUST_LOG=rustfs=debug ./rustfs server /data
```

Look for log lines like:
```
DEBUG rustfs::storage::ecfs: PUT object with special characters in key: "a f+/file.txt"
DEBUG rustfs::storage::ecfs: LIST objects with special characters in prefix: "ES+net/"
```

### Issue: "NoSuchKey error but file exists"

**Symptom**:
- Upload: `PUT /bucket/test+file.txt` works
- List: `GET /bucket?prefix=test` shows: `test+file.txt`
- Get: `GET /bucket/test+file.txt` fails with NoSuchKey

**Root Cause**: Key was stored with one encoding, requested with another.

**Diagnosis**:
```bash
# Check what name is actually stored
mc ls --recursive myrustfs/bucket/

# Try different encodings
curl "http://localhost:9000/bucket/test+file.txt"    # Literal +
curl "http://localhost:9000/bucket/test%2Bfile.txt"  # Encoded +
curl "http://localhost:9000/bucket/test%20file.txt"  # Space (if + was meant as space)
```

**Solution**: Use a consistent S3 client/SDK for all operations.

### Issue: "Special characters work in CLI but not in UI"

**Root Cause**: This is a UI bug. The backend (RustFS) handles special characters correctly when accessed via proper S3 clients.

**Verification**:
```bash
# These should all work:
mc cp file.txt "myrustfs/bucket/test with spaces/file.txt"
mc ls "myrustfs/bucket/test with spaces/"

aws --endpoint-url=http://localhost:9000 s3 cp file.txt "s3://bucket/test with spaces/file.txt"
aws --endpoint-url=http://localhost:9000 s3 ls "s3://bucket/test with spaces/"
```

**Solution**: Report as UI bug. Use CLI for now.

## Best Practices

### 1. Use Simple Names When Possible

Avoid special characters if you don't need them:
- ✅ Good: `my-file.txt`, `data_2024.json`, `report-final.pdf`
- ⚠️ Acceptable but complex: `my file.txt`, `data+backup.json`, `report (final).pdf`

### 2. Always Use S3 SDKs/Clients

Don't try to build raw HTTP requests yourself. Use:
- AWS CLI
- MinIO client (mc)
- AWS SDKs (Python/boto3, Go, Node.js, Java, etc.)
- Other S3-compatible SDKs

### 3. Understand URL Encoding

If you must work with URLs directly:
- **In URL paths**: Space=`%20`, Plus=`%2B`, `+` means literal plus
- **In query params**: Space=`%20` or `+`, Plus=`%2B`
- Use a URL encoding library in your language

### 4. Test Your Client

Before deploying:
```bash
# Test with spaces
mc cp test.txt "myrustfs/bucket/test with spaces/file.txt"
mc ls "myrustfs/bucket/test with spaces/"

# Test with plus
mc cp test.txt "myrustfs/bucket/test+plus/file.txt"
mc ls "myrustfs/bucket/test+plus/"

# Test with mixed
mc cp test.txt "myrustfs/bucket/test with+mixed/file.txt"
mc ls "myrustfs/bucket/test with+mixed/"
```

## Technical Details

### How RustFS Handles Special Characters

1. **Request Reception**: Client sends HTTP request with URL-encoded path
   ```
   PUT /bucket/test%20file.txt
   ```

2. **URL Decoding**: s3s library decodes the path
   ```rust
   let decoded = urlencoding::decode("/bucket/test%20file.txt")
   // Result: "/bucket/test file.txt"
   ```

3. **Storage**: Object stored with decoded name
   ```
   Stored as: "test file.txt"
   ```

4. **Retrieval**: Object retrieved by decoded name
   ```rust
   let key = "test file.txt";  // Already decoded by s3s
   store.get_object(bucket, key)
   ```

5. **Response**: Key returned in response (decoded)
   ```xml
   <Key>test file.txt</Key>
   ```

6. **Client Display**: S3 clients display the decoded name
   ```
   Shows: test file.txt
   ```

### URL Encoding Standards

RustFS follows:
- **RFC 3986**: URI Generic Syntax
- **AWS S3 API**: Object key encoding rules
- **HTTP/1.1**: URL encoding in request URIs

Key points:
- Keys are UTF-8 strings
- URL encoding is only for HTTP transport
- Keys are stored and compared in decoded form

## FAQs

**Q: Can I use spaces in object names?**  
A: Yes, but use an S3 SDK which handles encoding automatically.

**Q: Why does `+` not work as a space?**  
A: In URL paths, `+` represents a literal plus sign. Only in query parameters does `+` mean space. Use `%20` for spaces in paths.

**Q: Does RustFS support Unicode in object names?**  
A: Yes, object names are UTF-8 strings. They support any valid UTF-8 character.

**Q: What characters are forbidden?**  
A: Control characters (null byte, newline, carriage return) are rejected. All printable characters are allowed.

**Q: How do I fix "UI can't list folder" issue?**  
A: Use the CLI (mc or aws-cli) instead. This is a UI bug, not a backend issue.

**Q: Why do some clients work but others don't?**  
A: Proper S3 SDKs handle encoding correctly. Custom clients may have bugs. Always use official SDKs.

## Getting Help

If you encounter issues:

1. **Check this guide first**
2. **Verify you're using an S3 SDK** (not raw HTTP)
3. **Test with mc client** to isolate if issue is backend or client
4. **Enable debug logging** on RustFS: `RUST_LOG=rustfs=debug`
5. **Report issues** at: https://github.com/rustfs/rustfs/issues

Include in bug reports:
- Client/SDK used (and version)
- Exact object name causing issue
- Whether mc client works
- Debug logs from RustFS

---

**Last Updated**: 2025-12-09  
**RustFS Version**: 0.0.5+  
**Related Documents**:
- [Special Characters Analysis](./special-characters-in-path-analysis.md)
- [Special Characters Solution](./special-characters-solution.md)
