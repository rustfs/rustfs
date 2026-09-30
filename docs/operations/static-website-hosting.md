# Static website hosting

Set `RUSTFS_WEBSITE_DOMAINS` to one or more comma-separated base domains on the S3 listener. For example, `RUSTFS_WEBSITE_DOMAINS=website.example.com` serves bucket `photos` at `photos.website.example.com`. Configure wildcard DNS and a TLS certificate covering the bucket hostnames. The website domain must not overlap `RUSTFS_SERVER_DOMAINS`; startup rejects overlapping values.

Website routing is enabled only when the environment variable is set. Keep the public website hostname in the reverse proxy's `Host` header. The S3 API hostname and path-style endpoints continue to serve ordinary S3 requests, including object GET/HEAD, listing, version selection, and XML errors. A website hostname accepts GET and HEAD only.

Configure the bucket with `PutBucketWebsite`. An index document is required unless `RedirectAllRequestsTo` is configured. A trailing slash resolves to the index document; a missing slash redirects to the trailing-slash URL when an index exists. The website endpoint uses the configured error document for 403 and 404 responses and evaluates routing rules in configuration order. Object metadata `x-amz-website-redirect-location` redirects only on the website endpoint.

Website requests are anonymous. Grant `s3:GetObject` through the bucket policy for every page to be served, including index and error documents. Explicit deny and public-access restrictions still apply. A private error document is not exposed as a fallback. Use the S3 endpoint for authenticated reads and writes.

For HTTPS deployments, set the `Protocol` field explicitly in website redirect rules and `RedirectAllRequestsTo` when the proxy sends HTTP to RustFS. Otherwise, redirects use the protocol visible to the S3 listener.
